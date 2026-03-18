# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f19a95ac-ac82-44b4-afc6-532d355611a3",
# META       "default_lakehouse_name": "LH_HappyBooking",
# META       "default_lakehouse_workspace_id": "cceb4779-898a-4db8-bb21-94933a54b4b1",
# META       "known_lakehouses": [
# META         {
# META           "id": "f19a95ac-ac82-44b4-afc6-532d355611a3"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "54304a2c-2ea5-413d-91e3-0c284b744ea0",
# META       "known_warehouses": [
# META         {
# META           "id": "54304a2c-2ea5-413d-91e3-0c284b744ea0",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Enriched Processes

# MARKDOWN ********************

# ## Currency

# CELL ********************

from pyspark.sql import functions as F

# --- STEP 1: COUNTRY - CURRENCY MATCHING LIST ---
country_to_currency = [
    ("ARGENTINA", "ARS"), ("AUSTRALIA", "AUD"), ("AUSTRIA", "EUR"), ("BELGIUM", "EUR"),
    ("BRAZIL", "BRL"), ("BULGARIA", "BGN"), ("CANADA", "CAD"), ("CHILE", "CLP"),
    ("CHINA", "CNY"), ("COLOMBIA", "COP"), ("CROATIA", "EUR"), ("CZECH REPUBLIC", "CZK"),
    ("DENMARK", "DKK"), ("EGYPT", "EGP"), ("FINLAND", "EUR"), ("FRANCE", "EUR"),
    ("GERMANY", "EUR"), ("GREECE", "EUR"), ("HUNGARY", "HUF"), ("INDIA", "INR"),
    ("INDONESIA", "IDR"), ("IRELAND", "EUR"), ("ISRAEL", "ILS"), ("ITALY", "EUR"),
    ("JAPAN", "JPY"), ("KENYA", "KES"), ("MALAYSIA", "MYR"), ("MEXICO", "MXN"),
    ("MOROCCO", "MAD"), ("NETHERLANDS", "EUR"), ("NEW ZEALAND", "NZD"), ("NIGERIA", "NGN"),
    ("NORWAY", "NOK"), ("PERU", "PEN"), ("PHILIPPINES", "PHP"), ("POLAND", "PLN"),
    ("PORTUGAL", "EUR"), ("ROMANIA", "RON"), ("SAUDI ARABIA", "SAR"), ("SERBIA", "RSD"),
    ("SINGAPORE", "SGD"), ("SLOVAKIA", "EUR"), ("SOUTH AFRICA", "ZAR"), ("SOUTH KOREA", "KRW"),
    ("SPAIN", "EUR"), ("SWEDEN", "SEK"), ("SWITZERLAND", "CHF"), ("THAILAND", "THB"),
    ("TURKEY", "TRY"), ("UNITED ARAB EMIRATES", "AED"), ("UNITED KINGDOM", "GBP"),
    ("UNITED STATES", "USD"), ("VIETNAM", "VND")
]

df_mapping = spark.createDataFrame(country_to_currency, ["country_name", "currency_code_map"])

# --- STEP 2: PREPARE DATA ---
df_silver = spark.table("silver_hotel_cleaned")
df_currency = spark.table("bronze_api_currency")
df_weather = spark.table("bronze_api_weather")

# Averages for fallback 
avg_rates = df_currency.groupBy("currency_code").agg(
    F.avg("usd_rate").alias("avg_usd_rate")
).withColumnRenamed("currency_code", "avg_currency_code")

# Add a map to the hotel table
df_silver_with_code = df_silver.join(
    df_mapping, 
    df_silver.country == df_mapping.country_name, 
    "left"
).alias("hotels")

# Prepare the exchange rate table with an alias
df_currency_alias = df_currency.alias("rates")

# --- STEP 3: JOINING ---
df_with_currency = df_silver_with_code.join(
    df_currency_alias,
    (F.col("hotels.currency_code_map") == F.col("rates.currency_code")) & 
    (F.col("hotels.booking_date") == F.col("rates.rate_date")),
    "left"
).join(
    avg_rates,
    F.col("hotels.currency_code_map") == F.col("avg_currency_code"),
    "left"
)

# --- STEP 4: USD CALCULATION ---
df_final_enriched = df_with_currency.withColumn(
    "effective_rate",
    F.when(F.col("hotels.currency_code_map") == "USD", F.lit(1.0))
     .otherwise(F.coalesce(F.col("rates.usd_rate"), F.col("avg_usd_rate"), F.lit(1.0)))
).withColumn(
    "total_price_usd",
    F.col("hotels.total_price") * F.col("effective_rate")
)

# --- STEP 5: RESOLVING AND SAVING COLUMN CONFLICTS ---
final_columns = [F.col("hotels.*"), F.col("effective_rate"), F.col("total_price_usd")]

df_hotel = df_final_enriched.select(*final_columns)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Weather

# CELL ********************

# 6. JOIN OPERATION
# We are matching the city name and check-in date. 
# We are using a LEFT join so that hotels without weather information are not deleted.
df_gold_raw = df_hotel.join(
    df_weather,
    (df_hotel.city == df_weather.city_name) & 
    (df_hotel.checkin_date == df_weather.weather_date),
    "left"
)

# 7. MANAGING MISSING WEATHER DATA (Imputation)
# Let's fill in null values for cities with missing data.
df_gold_final = df_gold_raw.fillna({
    "temp_max": 22.5,  # Assumption of average global temperature
    "rain_sum": 0.0     # If there is no data, let us assume it did not rain.
})

# 8. EXTRA ANALYTICAL COLUMNS (KPIs)
# Season and price/night metrics that will be useful when conducting analysis
df_gold_final = df_gold_final.withColumn(
    "is_rainy", F.when(F.col("rain_sum") > 0.5, True).otherwise(False)
).withColumn(
    "price_category", 
    F.when(F.col("total_price_usd") < 150, "Budget")
     .when(F.col("total_price_usd") < 400, "Mid-Range")
     .otherwise("Luxury")
)

# 9. CLEAN UP UNNECESSARY DUPLICATE COLUMNS
# Let's remove columns such as 'city_name' and 'weather_date' that appear after the join.
cols_to_drop = ["city_name", "weather_date"]
df_gold_final = df_gold_final.drop(*cols_to_drop)

# 10. SAVE THE GOLD TABLE
df_gold_final.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_hotel_weather_analytics")

print("🏆 The GOLD TABLE is ready!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
