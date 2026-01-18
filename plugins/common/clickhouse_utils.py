import logging
from datetime import datetime

from clickhouse_connect.driver.client import Client as ClickHouseClient
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class ClassifiedNews(BaseModel):
    """Классифицированная новость для записи в ClickHouse."""

    source: str
    title: str
    url: str
    category: str
    confidence: float
    model_name: str
    published_at: datetime
    s3_path_raw: str
    s3_path_processed: str


class ApiUsage(BaseModel):
    """Статистика использования API."""

    api_name: str
    tokens_used: int
    latency_ms: int
    status_code: int
    dag_id: str
    task_id: str


class ProcessingLog(BaseModel):
    """Лог обработки файлов."""

    raw_file_path: str
    processed_file_path: str
    status: str
    items_count: int


def _to_rows(records: list[BaseModel]) -> tuple[list[list], list[str]]:
    """Конвертация Pydantic моделей в строки для ClickHouse."""
    if not records:
        return [], []
    columns = list(records[0].model_fields.keys())
    rows = [[record.model_dump()[col] for col in columns] for record in records]
    return rows, columns


def insert_classified_news(client: ClickHouseClient, records: list[ClassifiedNews]) -> None:
    """Вставка классифицированных новостей в ClickHouse."""
    rows, columns = _to_rows(records)
    if rows:
        logger.info('insert_classified_news: вставка %d строк в news_classified', len(rows))
        client.insert('news_classified', rows, column_names=columns)


def insert_api_usage(client: ClickHouseClient, records: list[ApiUsage]) -> None:
    """Вставка статистики использования API в ClickHouse."""
    rows, columns = _to_rows(records)
    if rows:
        logger.info('insert_api_usage: вставка %d строк в api_usage', len(rows))
        client.insert('api_usage', rows, column_names=columns)


def insert_processing_log(client: ClickHouseClient, records: list[ProcessingLog]) -> None:
    """Вставка лога обработки в ClickHouse."""
    rows, columns = _to_rows(records)
    if rows:
        logger.info('insert_processing_log: вставка %d строк в processing_log', len(rows))
        client.insert('processing_log', rows, column_names=columns)
