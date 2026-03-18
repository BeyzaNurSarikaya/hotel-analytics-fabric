# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from pyspark.sql import functions as F

# 1. Customer Model
df_customer = spark.sql("""
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
FROM gold_hotel_weather_analytics
WHERE customer_id IS NOT NULL
""")

df_customer.write.format("delta").mode("overwrite").saveAsTable("dim_customer")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2. Date Model
spark.sql("""
WITH date_range AS (
    SELECT 
        DATEADD(year, 0, MIN(booking_date)) as start_date,
        DATEADD(year, 0, MAX(booking_date)) as end_date
    FROM gold_hotel_weather_analytics
),
numbers AS (SELECT id AS num FROM range(0, 4000)) -- Yaklaşık 10 yıllık spine

SELECT 
    DATEADD(day, n.num, r.start_date) AS date_key,
    YEAR(DATEADD(day, n.num, r.start_date)) AS year,
    MONTH(DATEADD(day, n.num, r.start_date)) AS month,
    FORMAT_NUMBER(MONTH(DATEADD(day, n.num, r.start_date)), 0) AS month_name, -- Basit format
    QUARTER(DATEADD(day, n.num, r.start_date)) AS quarter
FROM date_range r
CROSS JOIN numbers n
WHERE DATEADD(day, n.num, r.start_date) <= r.end_date
""").write.format("delta").mode("overwrite").saveAsTable("dim_date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3. Hotel Model (Deduplication)
spark.sql("""
WITH unique_hotels AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY hotel_id ORDER BY hotel_id) as row_num
    FROM gold_hotel_weather_analytics
    WHERE hotel_id IS NOT NULL
)
SELECT * EXCEPT(row_num)
FROM unique_hotels
WHERE row_num = 1
""").write.format("delta").mode("overwrite").saveAsTable("dim_hotel")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 4. Booking Fact Model
spark.sql("""
SELECT
    booking_id,
    customer_id,
    hotel_id,
    total_price_usd,
    room_price,
    checkin_date,
    checkout_date,
    nights,
    booking_date,
    booking_status,
    temp_max,
    is_rainy
FROM gold_hotel_weather_analytics
WHERE booking_id IS NOT NULL
""").write.format("delta").mode("overwrite").saveAsTable("fct_booking")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
