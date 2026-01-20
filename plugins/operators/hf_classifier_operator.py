import logging
import time
from collections import Counter

import httpx
import yaml
from airflow.sdk import Context
from httpx import codes
from tenacity import before_sleep_log, retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from config.settings import hf_settings, processing_settings, storage_settings
from plugins.common.clickhouse_utils import ApiUsage, ClassifiedNews, insert_api_usage, insert_classified_news
from plugins.common.utils import parse_datetime
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
        self.timeout = hf_settings.timeout
        self.max_retries = hf_settings.max_retries
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

    def _classify_text(self, text: str, labels: list[str]) -> tuple[dict, int, int]:
        """Классификация через HuggingFace API с retry."""
        url = f'{self.api_url}/{self.model_name}'
        payload = {'inputs': text, 'parameters': {'candidate_labels': labels}}

        @retry(
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential(multiplier=2, min=2, max=60),
            retry=retry_if_exception_type(
                (
                    httpx.HTTPStatusError,
                    httpx.TimeoutException,
                    httpx.RemoteProtocolError,
                    httpx.ConnectError,
                )
            ),
            before_sleep=before_sleep_log(logger, logging.WARNING),
        )
        def _make_request() -> tuple[dict, int, int]:
            start = time.time()
            with httpx.Client(timeout=self.timeout) as client:
                response = client.post(url, headers=self._get_headers(), json=payload)
                latency_ms = int((time.time() - start) * 1000)

                if response.status_code == codes.TOO_MANY_REQUESTS:
                    logger.warning('⚠️ Rate limit, retrying...')
                response.raise_for_status()
                return response.json(), latency_ms, response.status_code

        return _make_request()

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

        classified_records: list[ClassifiedNews] = []
        api_usage_records: list[ApiUsage] = []
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
            if isinstance(result, list):
                result = result[0]

            if 'error' in result:
                logger.error('❌ API error для %s: %s', item_id, result['error'])
                continue

            if 'labels' in result and 'scores' in result:
                top_label, top_score = result['labels'][0], result['scores'][0]
            elif 'label' in result and 'score' in result:
                top_label, top_score = result['label'], result['score']
            else:
                logger.error('❌ Неожиданный формат ответа для %s: %s', item_id, result)
                continue

            if top_score < self.min_confidence:
                top_label = 'General Tech News'
                logger.info('🔻 %s: низкая уверенность (%.2f)', item_id, top_score)

            classified_records.append(
                ClassifiedNews(
                    source=item.get('source', 'unknown'),
                    title=item.get('original_title', ''),
                    url=item.get('url', ''),
                    category=top_label,
                    confidence=top_score,
                    model_name=self.model_name,
                    published_at=parse_datetime(item.get('published_at')),
                    s3_path_raw=f'raw-news/{source_file.replace("_processed", "")}',
                    s3_path_processed=f'{self.source_bucket}/{source_file}',
                )
            )

            api_usage_records.append(
                ApiUsage(
                    api_name='huggingface',
                    tokens_used=len(text) // 4,
                    latency_ms=latency_ms,
                    status_code=status,
                    dag_id=dag_id,
                    task_id=self.task_id,
                )
            )

            category_counts[top_label] += 1
            total_confidence += top_score
            logger.info('✅ %s → %s (%.2f) [%dms]', item_id, top_label, top_score, latency_ms)

        insert_classified_news(ch_client, classified_records)
        insert_api_usage(ch_client, api_usage_records)

        processed = len(classified_records)
        avg_confidence = total_confidence / processed if processed else 0
        logger.info('📊 Обработано: %d, средняя уверенность: %.2f', processed, avg_confidence)

        return {
            'processed': processed,
            'avg_confidence': round(avg_confidence, 3),
            'categories_distribution': dict(category_counts),
        }
