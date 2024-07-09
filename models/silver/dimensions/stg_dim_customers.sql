{{
    config(
        materialized='incremental',
        alias='stg_dim_customers',
        schema=var('silver_schema'),
        unique_key='customer_id',
        incremental_strategy='delete+insert'
    )
}}


SELECT
    customer_id,
    first_name,
    last_name,
    email,
    address,
    city,
    state,
    postal_code,
    phone_number,
    concat(concat(first_name, ' '), last_name) as full_name,
    getdate() as created_at
FROM
    {{ var('bronze_schema') }}.ext_customers