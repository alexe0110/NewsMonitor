import logging
import time
from collections import Counter
from datetime import UTC, datetime

import httpx
import yaml
from airflow.sdk import Context
from clickhouse_connect.driver.client import Client as ClickHouseClient
from httpx import codes
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from config.settings import hf_settings, processing_settings, storage_settings
from plugins.operators.base import BaseDataProcessingOperator

logger = logging.getLogger(__name__)


class HuggingFaceClassifierOperator(BaseDataProcessingOperator):
    """
    Оператор классификации новостей через HuggingFace Inference API.

    Читает обработанные новости из MinIO, классифицирует через HuggingFace API
    (zero-shot classification) и сохраняет результаты в ClickHouse.
    """

    def __init__(
        self,
        *,
        source_task_id: str = 'preprocess_news',
        categories_config: str | None = None,
        min_confidence: float | None = None,
        source_bucket: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.source_task_id = source_task_id
        self.model_name = hf_settings.model_name
        self.api_url = hf_settings.api_url
        self.api_token = hf_settings.api_token
        self.categories_config = categories_config or storage_settings.categories_config
        self.min_confidence = min_confidence or processing_settings.min_confidence
        self.source_bucket = source_bucket or storage_settings.processed_bucket

    def _load_categories(self) -> list[str]:
        """Загрузка категорий из YAML конфига."""
        with open(self.categories_config) as f:
            config = yaml.safe_load(f)
        return [cat['name'] for cat in config['categories'].values()]

    def _get_headers(self) -> dict:
        """Формирование заголовков для HF API."""
        headers = {'Content-Type': 'application/json'}
        if self.api_token:
            headers['Authorization'] = f'Bearer {self.api_token}'
        return headers

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.TimeoutException)),
    )
    def _classify_text(self, text: str, labels: list[str]) -> tuple[dict, int, int]:
        """Классификация через HuggingFace API."""
        url = f'{self.api_url}/{self.model_name}'
        payload = {'inputs': text, 'parameters': {'candidate_labels': labels}}

        start = time.time()
        with httpx.Client(timeout=30.0) as client:
            response = client.post(url, headers=self._get_headers(), json=payload)
            latency_ms = int((time.time() - start) * 1000)

            if response.status_code == codes.TOO_MANY_REQUESTS:
                logger.warning('⚠️ Rate limit, retrying...')
            response.raise_for_status()
            return response.json(), latency_ms, response.status_code

    def _parse_datetime(self, value: str | None) -> datetime:
        """Парсинг даты публикации."""
        if not value:
            return datetime.now(tz=UTC)
        if isinstance(value, str):
            return datetime.fromisoformat(value.replace('Z', '+00:00'))
        return value

    def _insert_results(self, ch_client: ClickHouseClient, classified: list[list], api_usage: list[list]) -> None:
        """Вставка результатов в ClickHouse."""
        if classified:
            ch_client.insert(
                'news_classified',
                classified,
                column_names=[
                    'source',
                    'title',
                    'url',
                    'category',
                    'confidence',
                    'model_name',
                    'published_at',
                    's3_path_raw',
                    's3_path_processed',
                ],
            )
        if api_usage:
            ch_client.insert(
                'api_usage',
                api_usage,
                column_names=['api_name', 'tokens_used', 'latency_ms', 'status_code', 'dag_id', 'task_id'],
            )

    def execute(self, context: Context) -> dict:
        """Выполнение классификации."""
        ti = context['task_instance']
        source_file = ti.xcom_pull(task_ids=self.source_task_id, key='processed_file')

        if not source_file:
            logger.warning('⚠️ Нет файла для классификации')
            return {'processed': 0, 'avg_confidence': 0, 'categories_distribution': {}}

        minio_client = self.get_minio_client()
        ch_client = self.get_clickhouse_client()

        logger.info('📖 Чтение: %s/%s', self.source_bucket, source_file)
        news_items = self.read_json_from_minio(minio_client, self.source_bucket, source_file)
        labels = self._load_categories()
        logger.info('📦 Загружено %d элементов, категории: %s', len(news_items), labels)

        classified_records: list[list] = []
        api_usage_records: list[list] = []
        category_counts: Counter = Counter()
        total_confidence = 0.0

        dag_id = context.get('dag').dag_id if context.get('dag') else 'unknown'

        for i, item in enumerate(news_items):
            item_id = item.get('id', f'unknown_{i}')
            text = item.get('text', '')

            if not text:
                logger.warning('⚠️ Пропуск %s: пустой текст', item_id)
                continue

            result, latency_ms, status = self._classify_text(text, labels)
            top_label, top_score = result['labels'][0], result['scores'][0]

            if top_score < self.min_confidence:
                top_label = 'General Tech News'
                logger.info('🔻 %s: низкая уверенность (%.2f)', item_id, top_score)

            classified_records.append(
                [
                    item.get('source', 'unknown'),
                    item.get('original_title', ''),
                    item.get('url', ''),
                    top_label,
                    float(top_score),
                    self.model_name,
                    self._parse_datetime(item.get('published_at')),
                    f'raw-news/{source_file.replace("_processed", "")}',
                    f'{self.source_bucket}/{source_file}',
                ]
            )

            api_usage_records.append(['huggingface', len(text) // 4, latency_ms, status, dag_id, self.task_id])

            category_counts[top_label] += 1
            total_confidence += top_score
            logger.info('✅ %s → %s (%.2f) [%dms]', item_id, top_label, top_score, latency_ms)

        self._insert_results(ch_client, classified_records, api_usage_records)

        processed = len(classified_records)
        avg_confidence = total_confidence / processed if processed else 0
        logger.info('📊 Обработано: %d, средняя уверенность: %.2f', processed, avg_confidence)

        return {
            'processed': processed,
            'avg_confidence': round(avg_confidence, 3),
            'categories_distribution': dict(category_counts),
        }
