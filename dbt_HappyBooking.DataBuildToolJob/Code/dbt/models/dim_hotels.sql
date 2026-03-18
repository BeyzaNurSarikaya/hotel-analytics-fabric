{{ config(materialized='table') }}

WITH unique_hotels AS (
    SELECT 
        hotel_id,
        hotel_name,
        hotel_type,
        star_rating,
        hotel_facilities,
        nearby_attractions,
        hotel_description,
        country_name,
        city,
        address,
        postal_code,
        latitude,
        longitude,
        -- Her hotel_id için satırları numaralandırır
        ROW_NUMBER() OVER (PARTITION BY hotel_id ORDER BY hotel_id) as row_num
    FROM {{ source('gold_layer', 'gold_hotel_weather_analytics') }}
    WHERE hotel_id IS NOT NULL
)

SELECT 
    hotel_id,
    hotel_name,
    hotel_type,
    star_rating,
    hotel_facilities,
    nearby_attractions,
    hotel_description,
    country_name,
    city,
    address,
    postal_code,
    latitude,
    longitude
FROM unique_hotels
WHERE row_num = 1 -- Sadece her otelin ilk karşılaşılan satırını al