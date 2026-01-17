import logging
from datetime import datetime

from config.settings import processing_settings, storage_settings
from plugins.operators.base import BaseDataProcessingOperator

logger = logging.getLogger(__name__)

# Маппинг источников на функции нормализации
NORMALIZERS = {
    'hackernews': lambda item, max_len: {
        'id': f'hn_{item.get("id")}',
        'text': _truncate(item.get('title', ''), max_len),
        'source': 'hackernews',
        'url': item.get('url'),
        'original_title': item.get('title'),
        'published_at': (
            datetime.fromtimestamp(item['time']).isoformat() if item.get('time') else None
        ),
    },
    'devto': lambda item, max_len: {
        'id': f'devto_{item.get("id")}',
        'text': _truncate(_build_devto_text(item), max_len),
        'source': 'devto',
        'url': item.get('url'),
        'original_title': item.get('title'),
        'published_at': item.get('published_at'),
    },
}


def _truncate(text: str, max_length: int) -> str:
    """Обрезает текст до максимальной длины."""
    if len(text) > max_length:
        return text[: max_length - 3] + '...'
    return text


def _build_devto_text(item: dict) -> str:
    """Собирает текст из Dev.to статьи."""
    title = item.get('title', '')
    description = item.get('description', '')
    return f'{title}. {description}' if description else title


class TextPreprocessingOperator(BaseDataProcessingOperator):
    """
    Оператор для предобработки новостей перед классификацией.

    Читает JSON файлы из raw-news bucket, объединяет title + description,
    обрезает текст и сохраняет в processed-news bucket.
    """

    def __init__(
        self,
        *,
        source_task_id: str = 'find_unprocessed_files',
        source_bucket: str | None = None,
        target_bucket: str | None = None,
        max_text_length: int | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.source_task_id = source_task_id
        self.source_bucket = source_bucket or storage_settings.raw_bucket
        self.target_bucket = target_bucket or storage_settings.processed_bucket
        self.max_text_length = max_text_length or processing_settings.max_text_length

    def _process_item(self, item: dict) -> dict:
        """Нормализация элемента новости."""
        source = item.get('source', 'unknown')
        normalizer = NORMALIZERS.get(source, NORMALIZERS['devto'])
        return normalizer(item, self.max_text_length)

    def _log_processing(
        self, ch_client, raw_files: list[str], processed_file: str, items_count: int
    ) -> None:
        """Логирование обработки в ClickHouse."""
        records = [
            [
                f'{self.source_bucket}/{filename}',
                f'{self.target_bucket}/{processed_file}',
                'success',
                items_count,
            ]
            for filename in raw_files
        ]
        ch_client.insert(
            'processing_log',
            records,
            column_names=['raw_file_path', 'processed_file_path', 'status', 'items_count'],
        )

    def execute(self, context) -> int:
        ti = context['task_instance']
        source_files = ti.xcom_pull(task_ids=self.source_task_id, key='source_files')

        if not source_files:
            logger.warning('⚠️ Нет файлов для обработки')
            return 0

        minio_client = self.get_minio_client()
        ch_client = self.get_clickhouse_client()
        processed_items: list[dict] = []

        for filename in source_files:
            logger.info('📖 Чтение: %s/%s', self.source_bucket, filename)
            raw_items = self.read_json_from_minio(minio_client, self.source_bucket, filename)
            logger.info('📦 Загружено %d элементов', len(raw_items))

            processed_items.extend(self._process_item(item) for item in raw_items)

        # Сохранение
        output_filename = f'{datetime.now().strftime("%Y-%m-%d_%H%M")}_processed.json'
        self.save_json_to_minio(minio_client, self.target_bucket, output_filename, processed_items)
        logger.info('✅ Сохранено %d → %s/%s', len(processed_items), self.target_bucket, output_filename)

        self._log_processing(ch_client, source_files, output_filename, len(processed_items))

        ti.xcom_push(key='processed_file', value=output_filename)
        return len(processed_items)
