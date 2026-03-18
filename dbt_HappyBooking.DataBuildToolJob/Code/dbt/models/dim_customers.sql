{{ config(materialized='table') }}

SELECT DISTINCT
    customer_id,
    first_name,
    last_name,
    full_name,
    email,
    gender,
    loyalty_level_customer,
    marketing_consent,
    registration_date
FROM {{ source('gold_layer', 'gold_hotel_weather_analytics') }}
WHERE customer_id IS NOT NULL