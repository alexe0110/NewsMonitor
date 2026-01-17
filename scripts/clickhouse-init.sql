CREATE DATABASE IF NOT EXISTS news_analytics;

USE news_analytics;

CREATE TABLE IF NOT EXISTS news_raw (
    id UUID DEFAULT generateUUIDv4(),
    source String,
    title String,
    url String,
    content String,
    published_at DateTime,
    s3_path String,
    collected_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (published_at, id)
PARTITION BY toYYYYMM(published_at);

CREATE TABLE IF NOT EXISTS news_classified (
    id UUID DEFAULT generateUUIDv4(),
    news_id UUID,
    category String,
    confidence Float32,
    model_name String,
    classified_at DateTime DEFAULT now(),
    s3_path_processed String
) ENGINE = MergeTree()
ORDER BY (classified_at, id)
PARTITION BY toYYYYMM(classified_at);

CREATE TABLE IF NOT EXISTS api_usage (
    id UUID DEFAULT generateUUIDv4(),
    timestamp DateTime DEFAULT now(),
    api_name String,
    tokens_used UInt32,
    latency_ms UInt32,
    status_code UInt16,
    dag_id String,
    task_id String
) ENGINE = MergeTree()
ORDER BY (timestamp, id)
PARTITION BY toYYYYMM(timestamp);