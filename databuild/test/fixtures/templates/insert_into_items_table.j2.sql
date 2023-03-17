{% macro select_columns(table_name) -%}
    ingestion_date, id, title
{%- endmacro %}

with raw_items as (
    SELECT data.*
    FROM VALUES
        ('2023-01-01', '1000000', 'foo title')
    AS data(ingestion_date, id, title)
)

INSERT OVERWRITE TABLE items
    SELECT
        {{ select_columns('items') }}
FROM raw_items
