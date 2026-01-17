import clickhouse_connect
from clickhouse_connect.driver.client import Client as ClickHouseClient
from minio import Minio

from config.settings import clickhouse_settings, minio_settings


def get_minio_client() -> Minio:
    return Minio(
        endpoint=minio_settings.endpoint,
        access_key=minio_settings.access_key,
        secret_key=minio_settings.secret_key,
        secure=minio_settings.secure,
    )


def get_clickhouse_client() -> ClickHouseClient:
    return clickhouse_connect.get_client(
        host=clickhouse_settings.host,
        username=clickhouse_settings.user,
        password=clickhouse_settings.password,
        database=clickhouse_settings.db,
    )
