with raw_events as (
    SELECT data.*
    FROM VALUES
        ('1000000', 'event payload')
    AS data(id, payload)
)

INSERT OVERWRITE TABLE events PARTITION (ingestion_date='2023-01-01')
    SELECT
        {{ helper.star(from_table='events', except_columns="ingestion_date") }}
FROM raw_events
