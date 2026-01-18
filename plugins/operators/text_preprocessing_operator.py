import logging
from datetime import UTC, datetime

from airflow.sdk import Context

from config.settings import processing_settings, storage_settings
from plugins.common.clickhouse_utils import ProcessingLog, insert_processing_log
from plugins.common.normalizers import NORMALIZERS
from plugins.operators.base import BaseDataProcessingOperator

logger = logging.getLogger(__name__)


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
        normalizer = NORMALIZERS.get(source)
        return normalizer(item, self.max_text_length).model_dump()

    def execute(self, context: Context) -> int:
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

        output_filename = f'{datetime.now(tz=UTC).strftime("%Y-%m-%d_%H%M")}_processed.json'
        self.save_json_to_minio(minio_client, self.target_bucket, output_filename, processed_items)
        logger.info('✅ Сохранено %d → %s/%s', len(processed_items), self.target_bucket, output_filename)

        processing_logs = [
            ProcessingLog(
                raw_file_path=f'{self.source_bucket}/{filename}',
                processed_file_path=f'{self.target_bucket}/{output_filename}',
                status='success',
                items_count=len(processed_items),
            )
            for filename in source_files
        ]
        insert_processing_log(ch_client, processing_logs)

        ti.xcom_push(key='processed_file', value=output_filename)
        return len(processed_items)
