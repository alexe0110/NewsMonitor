from airflow.models import BaseOperator
from minio import Minio

from plugins.common.clients import get_clickhouse_client, get_minio_client
from plugins.common.minio_utils import read_json, save_json


class BaseDataProcessingOperator(BaseOperator):
    """
    Базовый оператор с общей логикой работы с MinIO и ClickHouse.
    """

    def get_minio_client(self) -> Minio:
        """Получение MinIO клиента."""
        return get_minio_client()

    def get_clickhouse_client(self):
        """Получение ClickHouse клиента."""
        return get_clickhouse_client()

    def read_json_from_minio(self, client: Minio, bucket: str, filename: str) -> list:
        """Чтение JSON файла из MinIO."""
        return read_json(client, bucket, filename)

    def save_json_to_minio(
        self, client: Minio, bucket: str, filename: str, data: list
    ) -> None:
        """Сохранение JSON данных в MinIO."""
        save_json(client, bucket, filename, data)
