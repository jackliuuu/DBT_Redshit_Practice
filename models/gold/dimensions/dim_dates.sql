{{
    config(
        materialized='incremental',
        alias='dim_dates',
        schema = var('gold_schema'),
        unique_key='date_id',
        incremental_strategy='delete+insert'
    )
}}

SELECT
    date_id,
    "date",
    "day",
    "month",
    "year",
    weekday,
    created_at
FROM
    {{ ref('stg_dim_dates') }}
