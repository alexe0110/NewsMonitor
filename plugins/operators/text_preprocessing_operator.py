import io
import json
import logging
import os
from datetime import datetime

import clickhouse_connect
from airflow.models import BaseOperator
from minio import Minio

logger = logging.getLogger(__name__)


class TextPreprocessingOperator(BaseOperator):
    """
    Оператор для предобработки новостей перед классификацией.

    Читает JSON файлы из raw-news bucket, объединяет title + description,
    обрезает текст и сохраняет в processed-news bucket.

    Args:
        source_bucket: Bucket для чтения сырых данных.
        target_bucket: Bucket для сохранения обработанных данных.
        source_files: Список файлов для обработки.
        max_text_length: Максимальная длина текста (default: 2000).
    """

    template_fields = ('source_files',)

    def __init__(
        self,
        *,
        source_bucket: str = 'raw-news',
        target_bucket: str = 'processed-news',
        source_files: list[str],
        max_text_length: int = 2000,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.source_bucket = source_bucket
        self.target_bucket = target_bucket
        self.source_files = source_files
        self.max_text_length = max_text_length

    def _get_minio_client(self) -> Minio:
        """Создание MinIO клиента из environment variables."""
        return Minio(
            endpoint=os.getenv('MINIO_ENDPOINT', 'minio:9000'),
            access_key=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
            secret_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
            secure=False,
        )

    def _get_clickhouse_client(self):
        """Создание ClickHouse клиента."""
        return clickhouse_connect.get_client(
            host=os.getenv('CLICKHOUSE_HOST', 'clickhouse'),
            username=os.getenv('CLICKHOUSE_USER', 'default'),
            password=os.getenv('CLICKHOUSE_PASSWORD', 'clickhouse'),
            database=os.getenv('CLICKHOUSE_DB', 'news_analytics'),
        )

    def _read_json_from_minio(self, client: Minio, bucket: str, filename: str) -> list:
        """Чтение JSON файла из MinIO."""
        response = client.get_object(bucket, filename)
        data = json.loads(response.read().decode('utf-8'))
        response.close()
        response.release_conn()
        return data

    def _save_json_to_minio(self, client: Minio, bucket: str, filename: str, data: list) -> None:
        """Сохранение JSON данных в MinIO."""
        json_bytes = json.dumps(data, indent=2, ensure_ascii=False).encode('utf-8')
        client.put_object(
            bucket_name=bucket,
            object_name=filename,
            data=io.BytesIO(json_bytes),
            length=len(json_bytes),
            content_type='application/json',
        )

    def _log_processing(
        self,
        ch_client,
        raw_files: list[str],
        processed_file: str,
        items_count: int,
    ) -> None:
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
            column_names=[
                'raw_file_path',
                'processed_file_path',
                'status',
                'items_count',
            ],
        )

    def _process_item(self, item: dict) -> dict:
        """Обработка одного элемента новости."""
        source = item.get('source', 'unknown')

        if source == 'hackernews':
            item_id = f'hn_{item.get("id")}'
            text = item.get('title', '')
            published_at = datetime.fromtimestamp(item.get('time', 0)).isoformat() if item.get('time') else None
        else:  # devto
            item_id = f'devto_{item.get("id")}'
            title = item.get('title', '')
            description = item.get('description', '')
            text = f'{title}. {description}' if description else title
            published_at = item.get('published_at')

        # Обрезаем текст до max_text_length
        if len(text) > self.max_text_length:
            text = text[: self.max_text_length - 3] + '...'

        return {
            'id': item_id,
            'text': text,
            'source': source,
            'url': item.get('url'),
            'original_title': item.get('title'),
            'published_at': published_at,
        }

    def execute(self, context) -> int:
        """Выполнение оператора."""
        # Проверка на пустой список файлов
        if not self.source_files:
            logger.warning('⚠️ Нет файлов для обработки, пропуск')
            return 0

        minio_client = self._get_minio_client()
        ch_client = self._get_clickhouse_client()
        processed_items: list[dict] = []

        for filename in self.source_files:
            logger.info('📖 Чтение файла: %s/%s', self.source_bucket, filename)

            try:
                raw_items = self._read_json_from_minio(minio_client, self.source_bucket, filename)
                logger.info('📦 Загружено %d элементов из %s', len(raw_items), filename)

                for item in raw_items:
                    processed = self._process_item(item)
                    processed_items.append(processed)

            except Exception as e:
                logger.error('❌ Ошибка при обработке %s: %s', filename, e)
                raise

        # Сохранение в MinIO
        output_filename = f'{datetime.now().strftime("%Y-%m-%d_%H%M")}_processed.json'
        self._save_json_to_minio(minio_client, self.target_bucket, output_filename, processed_items)
        logger.info(
            '✅ Сохранено %d элементов в %s/%s',
            len(processed_items),
            self.target_bucket,
            output_filename,
        )

        self._log_processing(ch_client, self.source_files, output_filename, len(processed_items))
        logger.info('📝 Записано в processing_log')

        # Push в XCom
        ti = context.get('task_instance')
        if ti:
            ti.xcom_push(key='processed_file', value=output_filename)

        return len(processed_items)
