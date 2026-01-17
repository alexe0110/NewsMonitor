from pydantic_settings import BaseSettings, SettingsConfigDict


class MinioSettings(BaseSettings):
    endpoint: str = 'minio:9000'
    access_key: str = 'minioadmin'
    secret_key: str = 'minioadmin'
    secure: bool = False

    model_config = SettingsConfigDict(env_prefix='MINIO_')

class ClickHouseSettings(BaseSettings):
    host: str = 'clickhouse'
    user: str = 'default'
    password: str = 'clickhouse'
    db: str = 'news_analytics'

    model_config = SettingsConfigDict(env_prefix='CLICKHOUSE_')

class HuggingFaceSettings(BaseSettings):
    model_name: str = 'facebook/bart-large-mnli'
    api_token: str | None = None
    api_url: str = 'https://api-inference.huggingface.co/models'

    model_config = SettingsConfigDict(env_prefix='HF_')

class StorageSettings(BaseSettings):
    raw_bucket: str = 'raw-news'
    processed_bucket: str = 'processed-news'
    categories_config: str = '/opt/airflow/config/categories.yaml'


class ProcessingSettings(BaseSettings):
    max_text_length: int = 2000
    min_confidence: float = 0.3
    fetch_limit: int = 30


minio_settings = MinioSettings()
clickhouse_settings = ClickHouseSettings()
hf_settings = HuggingFaceSettings()
storage_settings = StorageSettings()
processing_settings = ProcessingSettings()
