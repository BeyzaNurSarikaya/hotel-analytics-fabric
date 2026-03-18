# Fabric notebook source

# METADATA *********************

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
# META     }
# META   }
# META }

# MARKDOWN ********************

# # API Ingestion

# MARKDOWN ********************

# ## Currency API

# CELL ********************

from pyspark.sql import functions as F

# 1. Let's define the cleaning function
# regexp_replace(column, "[^a-zA-Z\s]", "") -> Removes everything except letters and spaces
# trim() -> Removes leading and trailing spaces
# upper() -> Converts everything to uppercase

def clean_country_name(df):
    return df.withColumn("country", 
        F.upper(
            F.trim(
                F.regexp_replace(F.col("country"), "[^a-zA-Z\s]", "")
            )
        )
    )

# 2. Pull by clearing batch and stream tables
df_batch_clean = clean_country_name(spark.table("bronze_hotel_batch")).select("country")
df_stream_clean = clean_country_name(spark.table("bronze_hotel_stream")).select("country")

# 3. Combine and take the unique ones
unique_countries_df = df_batch_clean.union(df_stream_clean) \
    .filter("country IS NOT NULL AND country != ''") \
    .distinct() \
    .orderBy("country")

# 4. Show the result
unique_countries_df.show(truncate=False)

# Import the list into Python
final_countries = [row['country'] for row in unique_countries_df.collect()]
print(f"Country List ({len(final_countries)} points):", final_countries)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# 1. Configure Spark's history policy (Critical step to avoid errors)
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# 2. Merge the tables and clean the data
df_batch = spark.table("bronze_hotel_batch").select("booking_date")
df_stream = spark.table("bronze_hotel_stream").select("booking_date")
df_all = df_batch.union(df_stream)

# 3. Trim (space removal) added to new date conversion
# We are removing spaces such as ' ' at the end of the date
df_fixed = df_all.withColumn("clean_date", 
    F.coalesce(
        F.to_date(F.trim(F.col("booking_date")), "yyyy-MM-dd"),
        F.to_date(F.trim(F.col("booking_date")), "dd/MM/yyyy"),
        F.to_date(F.trim(F.col("booking_date")), "MM/dd/yyyy")
    )
)

# 4. Calculate the range
date_stats = df_fixed.filter(F.col("clean_date").isNotNull()).select(
    F.min("clean_date").alias("start"),
    F.max("clean_date").alias("end")
).collect()

min_date = date_stats[0]['start']
max_date = date_stats[0]['end']

print(f"✅ Successfully calculated!")
print(f"📌 Start Date: {min_date}")
print(f"📌 End Date: {max_date}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
from pyspark.sql import Row

# 1. API Ayarları
# Tarih aralığımızı senin bulduğun değişkenlerden alıyoruz
api_url = f"https://api.frankfurter.app/{min_date}..{max_date}?from=USD"

print(f"🔄 {min_date} - {max_date} Global rates are being lowered for the period...")

try:
    # Send a request to the API
    response = requests.get(api_url)
    
    if response.status_code == 200:
        data = response.json()
        rates_history = data.get('rates', {})
        
        currency_rows = []
        
        # Flatten the JSON structure
        # { '2024-01-01': {'EUR': 0.91, 'TRY': 30.1}, '2024-01-02': {...} }
        for r_date, currencies in rates_history.items():
            for c_code, r_value in currencies.items():
                currency_rows.append(Row(
                    rate_date=r_date,       # Join key 1
                    currency_code=c_code,   # Join key 2
                    usd_rate=float(r_value) # How many units are in 1 USD?
                ))
        
        # 2. Convert to Spark Table and Save
        if currency_rows:
            df_currency_final = spark.createDataFrame(currency_rows)
            df_currency_final.write.format("delta").mode("overwrite").saveAsTable("bronze_api_currency")
            
            print(f"✅ Success! {len(currency_rows)} currency records have been downloaded.")
            print(f"📊 Table: bronze_api_currency")
        else:
            print("⚠️ The API returned empty data.")
            
    else:
        print(f"❌ API error: {response.status_code}")

except Exception as e:
    print(f"❌ An unexpected error: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Let's convert the mapping_data in the Python variable to a Spark DataFrame.
mapping_data = [
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

df_my_mapping = spark.createDataFrame(mapping_data, ["country_name", "currency_code"])

# 2. Let's retrieve the list of rates we downloaded from the API.
df_downloaded_currs = spark.table("bronze_api_currency").select("currency_code").distinct()

# 3. Let's Compare
# Which countries' exchange rate information did not come from the API?
df_missing = df_my_mapping.join(
    df_downloaded_currs, 
    df_my_mapping.currency_code == df_downloaded_currs.currency_code, 
    "left_anti"
)

print("❌ Countries and Currencies NOT FOUND in the API:")
df_missing.show(n=100)

print("✅ Number of countries with corresponding information in the API (with currency information):")
print(df_my_mapping.join(df_downloaded_currs, "currency_code").select("country_name").distinct().count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Let us take the dates in the current exchange rate table as a basis
distinct_dates = spark.table("bronze_api_currency").select("rate_date").distinct()

# 2. Let us define fixed rates
fallback_data = [
    ("ARS", 850.0), ("CLP", 950.0), ("COP", 3900.0), ("EGP", 48.0),
    ("KES", 130.0), ("MAD", 10.0), ("NGN", 1400.0), ("PEN", 3.7),
    ("SAR", 3.75), ("RSD", 107.0), ("AED", 3.67), ("VND", 24500.0),
    ("USD", 1.0)
]
df_fallbacks = spark.createDataFrame(fallback_data, ["currency_code", "usd_rate"])

# 3. Create the "data set to be added" by matching fixed exchange rates with dates.
# We make the column order the same as bronze_api_currency: rate_date, currency_code, usd_rate.
df_to_append = distinct_dates.crossJoin(df_fallbacks) \
    .select("rate_date", "currency_code", "usd_rate")

# 4. APPEND (Add) to the existing table
# Note: 'append' mode does not delete existing data, it writes to it.
df_to_append.write.format("delta").mode("append").saveAsTable("bronze_api_currency")

print("✅ Fixed rates have been successfully appended to the current table!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Let's convert the mapping_data in the Python variable to a Spark DataFrame.
mapping_data = [
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

df_my_mapping = spark.createDataFrame(mapping_data, ["country_name", "currency_code"])

# 2. Let's retrieve the list of rates we downloaded from the API.
df_downloaded_currs = spark.table("bronze_api_currency").select("currency_code").distinct()

# 3. Let's Compare
# Which countries' exchange rate information did not come from the API?
df_missing = df_my_mapping.join(
    df_downloaded_currs, 
    df_my_mapping.currency_code == df_downloaded_currs.currency_code, 
    "left_anti"
)

print("❌ Countries and Currencies NOT FOUND in the API:")
df_missing.show(n=100)

print("✅ Number of countries with corresponding information in the API (with currency information):")
print(df_my_mapping.join(df_downloaded_currs, "currency_code").select("country_name").distinct().count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
