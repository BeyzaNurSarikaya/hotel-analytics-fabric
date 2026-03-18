{{ config(materialized='table') }}

WITH date_range AS (
    SELECT 
        DATEADD(year, 0, MIN(booking_date)) as start_date,
        DATEADD(year, 0, MAX(booking_date)) as end_date
    FROM {{ source('gold_layer', 'gold_hotel_weather_analytics') }}
),


t0 AS (SELECT 1 AS n UNION ALL SELECT 1),
t1 AS (SELECT 1 AS n FROM t0 AS a CROSS JOIN t0 AS b),
t2 AS (SELECT 1 AS n FROM t1 AS a CROSS JOIN t1 AS b),
t3 AS (SELECT 1 AS n FROM t2 AS a CROSS JOIN t2 AS b),
t4 AS (SELECT 1 AS n FROM t3 AS a CROSS JOIN t3 AS b),
numbers AS (SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS num FROM t4),

date_spine AS (
    SELECT 
        DATEADD(day, n.num, r.start_date) AS calendar_date,
        r.end_date
    FROM date_range r
    CROSS JOIN numbers n
    WHERE DATEADD(day, n.num, r.start_date) <= r.end_date
)

SELECT
    calendar_date AS date_key,
    YEAR(calendar_date) AS year,
    MONTH(calendar_date) AS month,
    CAST(DATENAME(month, calendar_date) AS VARCHAR(50)) AS month_name,
    DATEPART(quarter, calendar_date) AS quarter,
    DATEPART(week, calendar_date) AS week_of_year,
    CAST(DATENAME(weekday, calendar_date) AS VARCHAR(50)) AS day_of_week_name,
    CASE WHEN DATEPART(weekday, calendar_date) IN (1, 7) THEN 1 ELSE 0 END AS is_weekend,
    CAST(
        CONCAT(
            CAST(YEAR(calendar_date) AS VARCHAR(4)), 
            '-', 
            CASE WHEN MONTH(calendar_date) < 10 THEN '0' ELSE '' END, 
            CAST(MONTH(calendar_date) AS VARCHAR(2))
        ) AS VARCHAR(10)
    ) AS year_month
FROM date_spine