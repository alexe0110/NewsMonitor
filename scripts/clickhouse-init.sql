CREATE DATABASE IF NOT EXISTS news_analytics;
USE news_analytics;

CREATE TABLE IF NOT EXISTS news_classified (
    id UUID DEFAULT generateUUIDv4(),
    source String,
    title String,
    url String,
    category String,
    confidence Float32,
    model_name String,
    published_at DateTime,
    classified_at DateTime DEFAULT now(),
    s3_path_raw String,
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

CREATE TABLE IF NOT EXISTS processing_log (
    id UUID DEFAULT generateUUIDv4(),
    raw_file_path String,
    processed_file_path String,
    processed_at DateTime DEFAULT now(),
    status String,
    items_count UInt32
) ENGINE = MergeTree()
ORDER BY (processed_at, id)
PARTITION BY toYYYYMM(processed_at);