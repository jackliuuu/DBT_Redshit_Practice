{{
    config(
        materialized='incremental',
        alias='dim_channels',
        schema=var('gold_schema'),
        unique_key='channel_id',
        incremental_strategy='delete+insert'
    )
}}

SELECT
    channel_id,
    channel_name,
    created_at
FROM
    {{ ref('stg_dim_channels') }}
