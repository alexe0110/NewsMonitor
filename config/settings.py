from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class MinioSettings(BaseSettings):
    access_key: str
    secret_key: str
    endpoint: str = 'minio:9000'
    secure: bool = False

    model_config = SettingsConfigDict(env_prefix='MINIO_')


class ClickHouseSettings(BaseSettings):
    user: str
    password: str
    db: str = 'news_analytics'
    host: str = 'clickhouse'

    model_config = SettingsConfigDict(env_prefix='CLICKHOUSE_')


class HuggingFaceSettings(BaseSettings):
    model_name: str = 'MoritzLaurer/deberta-v3-large-zeroshot-v2.0'
    api_token: str | None = Field(None, description='Not required, but improves rate limit')
    api_url: str = 'https://router.huggingface.co/models'

    model_config = SettingsConfigDict(env_prefix='HF_')


class StorageSettings(BaseSettings):
    raw_bucket: str = 'raw-news'
    processed_bucket: str = 'processed-news'
    categories_config: str = '/opt/airflow/config/categories.yaml'


class ProcessingSettings(BaseSettings):
    max_text_length: int = 2000
    min_confidence: float = 0.3
    fetch_limit: int = 30


minio_settings = MinioSettings()            # type: ignore[missing-argument]
clickhouse_settings = ClickHouseSettings()  # type: ignore[missing-argument]
hf_settings = HuggingFaceSettings()
storage_settings = StorageSettings()
processing_settings = ProcessingSettings()
