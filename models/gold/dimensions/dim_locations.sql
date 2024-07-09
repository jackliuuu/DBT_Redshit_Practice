{{
    config(
        materialized='incremental',
        alias='dim_locations',
        schema=var('gold_schema'),
        unique_key='location_id',
        incremental_strategy='delete+insert'
    )
}}

SELECT
    location_id,
    address,
    city,
    state,
    country,
    created_at
FROM
    {{ ref('stg_dim_locations') }}