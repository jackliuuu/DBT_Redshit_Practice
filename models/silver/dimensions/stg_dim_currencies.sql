{{
    config(
        materialized='incremental',
        alias='stg_dim_currencies',
        schema=var('silver_schema'),
        unique_key='currency_id',
        incremental_strategy='delete+insert'
    )
}}


SELECT
    currency_id,
    "name",
    iso3_code,
    active,
    getdate() as created_at
FROM
     {{ var('bronze_schema') }}.ext_currencies