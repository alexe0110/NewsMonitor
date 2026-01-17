import json
import logging
import os
import time
from collections import Counter
from datetime import datetime

import clickhouse_connect
import httpx
import yaml
from airflow.models import BaseOperator
from minio import Minio
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

HF_API_URL = "https://api-inference.huggingface.co/models"


class HuggingFaceClassifierOperator(BaseOperator):
    """
    Оператор классификации новостей через HuggingFace Inference API.

    Читает обработанные новости из MinIO, классифицирует через HuggingFace API
    (zero-shot classification) и сохраняет результаты в ClickHouse.

    Args:
        source_file: Имя файла в processed-news bucket.
        model_name: Название модели HuggingFace.
        categories_config: Путь к YAML файлу с категориями.
        min_confidence: Минимальный порог уверенности.
        source_bucket: Bucket для чтения данных.
    """

    template_fields = ("source_file",)

    def __init__(
        self,
        *,
        source_file: str,
        model_name: str = "facebook/bart-large-mnli",
        categories_config: str = "/opt/airflow/config/categories.yaml",
        min_confidence: float = 0.3,
        source_bucket: str = "processed-news",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.source_file = source_file
        self.model_name = os.getenv("HF_MODEL_NAME", model_name)
        self.categories_config = categories_config
        self.min_confidence = min_confidence
        self.source_bucket = source_bucket

    def _get_minio_client(self) -> Minio:
        return Minio(
            endpoint=os.getenv("MINIO_ENDPOINT", "minio:9000"),
            access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            secure=False,
        )

    def _get_clickhouse_client(self):
        return clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
            username=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", "clickhouse"),
            database=os.getenv("CLICKHOUSE_DB", "news_analytics"),
        )

    def _read_json_from_minio(self, client: Minio, bucket: str, filename: str) -> list:
        response = client.get_object(bucket, filename)
        data = json.loads(response.read().decode("utf-8"))
        response.close()
        response.release_conn()
        return data

    def _load_categories(self) -> list[str]:
        with open(self.categories_config) as f:
            config = yaml.safe_load(f)
        return [cat["name"] for cat in config["categories"].values()]

    def _get_headers(self) -> dict:
        headers = {"Content-Type": "application/json"}
        token = os.getenv("HF_API_TOKEN")
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.TimeoutException)),
    )
    def _classify_text(
        self, text: str, candidate_labels: list[str]
    ) -> tuple[dict, int, int]:
        """
        Классификация текста через HuggingFace API.

        Returns:
            tuple: (response_data, latency_ms, status_code)
        """
        url = f"{HF_API_URL}/{self.model_name}"
        payload = {
            "inputs": text,
            "parameters": {"candidate_labels": candidate_labels},
        }

        start_time = time.time()
        with httpx.Client(timeout=30.0) as client:
            response = client.post(url, headers=self._get_headers(), json=payload)

            latency_ms = int((time.time() - start_time) * 1000)
            status_code = response.status_code

            # Обработка rate limit
            if status_code == 429:
                logger.warning("⚠️ Rate limit, retrying...")
                response.raise_for_status()

            response.raise_for_status()
            return response.json(), latency_ms, status_code

    def _insert_classified_news(self, ch_client, records: list[list]) -> None:
        """Вставка классифицированных новостей в ClickHouse."""
        if not records:
            return

        ch_client.insert(
            "news_classified",
            records,
            column_names=[
                "source",
                "title",
                "url",
                "category",
                "confidence",
                "model_name",
                "published_at",
                "s3_path_raw",
                "s3_path_processed",
            ],
        )

    def _insert_api_usage(self, ch_client, records: list[list]) -> None:
        """Вставка записей использования API в ClickHouse."""
        if not records:
            return

        ch_client.insert(
            "api_usage",
            records,
            column_names=[
                "api_name",
                "tokens_used",
                "latency_ms",
                "status_code",
                "dag_id",
                "task_id",
            ],
        )

    def execute(self, context) -> dict:
        minio_client = self._get_minio_client()
        ch_client = self._get_clickhouse_client()

        # Загрузка данных
        logger.info("📖 Чтение файла: %s/%s", self.source_bucket, self.source_file)
        news_items = self._read_json_from_minio(
            minio_client, self.source_bucket, self.source_file
        )
        logger.info("📦 Загружено %d элементов", len(news_items))

        # Загрузка категорий
        candidate_labels = self._load_categories()
        logger.info("🏷️ Категории: %s", candidate_labels)

        classified_records: list[list] = []
        api_usage_records: list[list] = []
        category_counts: Counter = Counter()
        total_confidence = 0.0
        processed_count = 0

        dag_id = context.get("dag").dag_id if context.get("dag") else "unknown"
        task_id = self.task_id

        for i, item in enumerate(news_items):
            item_id = item.get("id", f"unknown_{i}")
            text = item.get("text", "")

            if not text:
                logger.warning("⚠️ Пропуск %s: пустой текст", item_id)
                continue

            try:
                result, latency_ms, status_code = self._classify_text(
                    text, candidate_labels
                )

                # Извлекаем top prediction
                top_label = result["labels"][0]
                top_score = result["scores"][0]

                # Проверка минимального порога
                if top_score < self.min_confidence:
                    top_label = "General Tech News"
                    logger.info(
                        "🔻 %s: низкая уверенность (%.2f), категория: %s",
                        item_id,
                        top_score,
                        top_label,
                    )

                # Парсинг даты публикации
                published_at = item.get("published_at")
                if published_at:
                    if isinstance(published_at, str):
                        published_at = datetime.fromisoformat(
                            published_at.replace("Z", "+00:00")
                        )
                else:
                    published_at = datetime.now()

                # Запись для news_classified
                classified_records.append(
                    [
                        item.get("source", "unknown"),
                        item.get("original_title", ""),
                        item.get("url", ""),
                        top_label,
                        float(top_score),
                        self.model_name,
                        published_at,
                        f"raw-news/{self.source_file.replace('_processed', '')}",
                        f"{self.source_bucket}/{self.source_file}",
                    ]
                )

                # Запись для api_usage
                tokens_estimate = len(text) // 4
                api_usage_records.append(
                    [
                        "huggingface",
                        tokens_estimate,
                        latency_ms,
                        status_code,
                        dag_id,
                        task_id,
                    ]
                )

                category_counts[top_label] += 1
                total_confidence += top_score
                processed_count += 1

                logger.info(
                    "✅ %s → %s (%.2f) [%dms]",
                    item_id,
                    top_label,
                    top_score,
                    latency_ms,
                )

            except httpx.HTTPStatusError as e:
                logger.error("❌ API ошибка для %s: %s", item_id, e)
                raise
            except Exception as e:
                logger.error("❌ Ошибка обработки %s: %s", item_id, e)
                raise

        # Сохранение в ClickHouse
        logger.info("💾 Сохранение в ClickHouse...")
        self._insert_classified_news(ch_client, classified_records)
        self._insert_api_usage(ch_client, api_usage_records)

        avg_confidence = total_confidence / processed_count if processed_count else 0

        logger.info(
            "📊 Обработано: %d, средняя уверенность: %.2f",
            processed_count,
            avg_confidence,
        )

        return {
            "processed": processed_count,
            "avg_confidence": round(avg_confidence, 3),
            "categories_distribution": dict(category_counts),
        }
