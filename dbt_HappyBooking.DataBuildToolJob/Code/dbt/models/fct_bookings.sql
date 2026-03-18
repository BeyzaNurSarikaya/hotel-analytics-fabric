{{ config(
    materialized='table',
    description='Gold tier booking table for reporting purposes'
) }}

WITH source_data AS (
    SELECT * FROM {{ source('gold_layer', 'gold_hotel_weather_analytics') }}
),

final AS (
    SELECT
        -- Unique Keys
        booking_id,
        customer_id,
        hotel_id,
        
        -- Financial Metrics
        total_price_usd,
        room_price,
        tax_amount,
        service_fee,
        discount_amount,
        
        -- Accommodation Information
        checkin_date,
        checkout_date,
        nights,
        booking_date,
        
        -- Further Information
        full_name,
        hotel_name,
        country_name,
        booking_status,
        
        -- Analytical Columns
        temp_max,
        is_rainy,
        price_category,
        star_rating
        
    FROM source_data
    WHERE booking_id IS NOT NULL -- Basic filter for data quality
)

SELECT * FROM final