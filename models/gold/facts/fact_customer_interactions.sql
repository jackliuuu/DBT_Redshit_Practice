{{
    config(
        materialized='view',
        alias='fact_customer_interactions',
        schema=var('gold_schema'),
        unique_key='interaction_id',
        incremental_stragey='delete+insert',
        primary_key='interaction_id',
        distribution='even'
    )
}}

WITH source_data AS (
    SELECT
        interaction_id,
        date_id,
        account_id,
        channel_id,
        interaction_type,
        interaction_rating
    FROM {{ ref('stg_fact_customer_interactions') }}
)

SELECT
    s.date_id,
    s.interaction_id,
    d.date as interaction_date,
    s.customer_id,
    c.first_name,
    c.last_name,
    c.full_name,
    c.city,
    c.state,
    c.postal_code,
    l.country,
    s.channel_id,
    ch.channel_name,
    s.interaction_type,
    s.interaction_rating
FROM source_data s
INNER JOIN {{ ref('dim_dates') }} AS d ON s.date_id = d.date_id
INNER JOIN {{ ref('dim_channels') }} AS ch ON s.channel_id = ch.channel_id

