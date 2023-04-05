CREATE TABLE IF NOT EXISTS events
(
    ingestion_date  STRING,
    id              STRING,
    payload         STRING
)
USING CSV
PARTITIONED BY (ingestion_date)
