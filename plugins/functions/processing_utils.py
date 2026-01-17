import logging

from config.settings import storage_settings
from plugins.common.clients import get_clickhouse_client, get_minio_client

logger = logging.getLogger(__name__)


def find_unprocessed_files(**context) -> int:
    """
    Поиск необработанных файлов в MinIO.

    Сравнивает файлы в raw-news bucket с записями в processing_log
    и возвращает список файлов, которые ещё не были обработаны.
    """
    minio_client = get_minio_client()
    ch_client = get_clickhouse_client()

    raw_files = {
        obj.object_name
        for obj in minio_client.list_objects(storage_settings.raw_bucket)
        if obj.object_name.endswith('.json') and not obj.object_name.startswith('.')
    }
    logger.info('📂 Найдено %d файлов в raw-news', len(raw_files))

    result = ch_client.query("SELECT DISTINCT raw_file_path FROM processing_log WHERE status = 'success'")
    processed_files = {row[0].replace(f'{storage_settings.raw_bucket}/', '') for row in result.result_rows}
    logger.info('✅ Уже обработано: %d файлов', len(processed_files))

    unprocessed = sorted(raw_files - processed_files)
    logger.info('📋 Необработанных файлов: %d', len(unprocessed))

    if unprocessed:
        logger.info('📄 Файлы для обработки: %s', unprocessed)
    else:
        logger.warning('⚠️ Нет новых файлов для обработки')

    if ti := context.get('task_instance'):
        ti.xcom_push(key='source_files', value=unprocessed)

    return len(unprocessed)
