"""Утилиты для отслеживания обработки файлов."""

import logging
import os

import clickhouse_connect
from minio import Minio

logger = logging.getLogger(__name__)


def _get_minio_client() -> Minio:
    """Создание MinIO клиента."""
    return Minio(
        endpoint=os.getenv('MINIO_ENDPOINT', 'minio:9000'),
        access_key=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
        secret_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
        secure=False,
    )


def _get_clickhouse_client():
    """Создание ClickHouse клиента."""
    return clickhouse_connect.get_client(
        host=os.getenv('CLICKHOUSE_HOST', 'clickhouse'),
        username=os.getenv('CLICKHOUSE_USER', 'default'),
        password=os.getenv('CLICKHOUSE_PASSWORD', 'clickhouse'),
        database=os.getenv('CLICKHOUSE_DB', 'news_analytics'),
    )


def find_unprocessed_files(**context) -> int:
    """
    Поиск необработанных файлов в MinIO.

    Сравнивает файлы в raw-news bucket с записями в processing_log
    и возвращает список файлов, которые ещё не были обработаны.

    Returns:
        Количество необработанных файлов.
    """
    minio_client = _get_minio_client()
    ch_client = _get_clickhouse_client()

    # Получаем список всех файлов в raw-news
    raw_files: set[str] = set()
    objects = minio_client.list_objects('raw-news')
    for obj in objects:
        filename = obj.object_name
        # Фильтруем только .json файлы, исключаем временные
        if filename.endswith('.json') and not filename.startswith('.'):
            raw_files.add(filename)

    logger.info('📂 Найдено %d файлов в raw-news', len(raw_files))

    # Получаем список уже обработанных файлов из ClickHouse
    result = ch_client.query("SELECT DISTINCT raw_file_path FROM processing_log WHERE status = 'success'")
    processed_paths = {row[0] for row in result.result_rows}

    # Преобразуем пути в имена файлов для сравнения
    processed_files = {path.replace('raw-news/', '') for path in processed_paths}

    logger.info('✅ Уже обработано: %d файлов', len(processed_files))

    # Находим разницу
    unprocessed = list(raw_files - processed_files)
    unprocessed.sort()

    logger.info('📋 Необработанных файлов: %d', len(unprocessed))

    if unprocessed:
        logger.info('📄 Файлы для обработки: %s', unprocessed)
    else:
        logger.warning('⚠️ Нет новых файлов для обработки')

    # Push в XCom
    ti = context.get('task_instance')
    if ti:
        ti.xcom_push(key='source_files', value=unprocessed)

    return len(unprocessed)
