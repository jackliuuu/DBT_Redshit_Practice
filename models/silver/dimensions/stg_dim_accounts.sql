{{
    config(
        materialized='incremental',
        alias='stg_dim_accounts',
        schema=var('silver_schema'),
        unique_key='account_id',
        incremental_strategy='delete+insert'
    )
}}


SELECT
    account_id,
    account_number,
    customer_id,
    account_type,
    account_balance,
    credit_score,
    getdate() as created_at
FROM
     {{ var('bronze_schema')}}.ext_accounts