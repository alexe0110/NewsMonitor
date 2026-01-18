from airflow.models import BaseOperator
from clickhouse_connect.driver.client import Client as ClickHouseClient
from minio import Minio

from plugins.common.clients import get_clickhouse_client, get_minio_client
from plugins.common.minio_utils import read_json, save_json


class BaseDataProcessingOperator(BaseOperator):
    """
    Базовый оператор с общей логикой работы с MinIO и ClickHouse.
    """

    @staticmethod
    def get_minio_client() -> Minio:
        """Получение MinIO клиента."""
        return get_minio_client()

    @staticmethod
    def get_clickhouse_client() -> ClickHouseClient:
        """Получение ClickHouse клиента."""
        return get_clickhouse_client()

    @staticmethod
    def read_json_from_minio(client: Minio, bucket: str, filename: str) -> list:
        """Чтение JSON файла из MinIO."""
        return read_json(client, bucket, filename)

    @staticmethod
    def save_json_to_minio(client: Minio, bucket: str, filename: str, data: list) -> None:
        """Сохранение JSON данных в MinIO."""
        save_json(client, bucket, filename, data)
