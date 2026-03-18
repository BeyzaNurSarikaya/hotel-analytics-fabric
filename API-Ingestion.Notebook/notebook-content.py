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
# META     }
# META   }
# META }

# MARKDOWN ********************

# # API Ingestion

# MARKDOWN ********************

# ## Weather API

# CELL ********************

from pyspark.sql import functions as F

# Cleaning Function: Remove spaces, convert to uppercase, remove special characters
def clean_city_names(df):
    return df.withColumn("city_clean", 
        F.upper(F.trim(F.regexp_replace(F.col("city"), r"[^a-zA-Z\s]", "")))
    )

# Let's take the cleaned cities for both batch and stream.
df_batch_cleaned = clean_city_names(spark.table("bronze_hotel_batch"))
df_stream_cleaned = clean_city_names(spark.table("bronze_hotel_stream"))

# Now let's take the unique locations from the cleaned names.
df_unique_cities = df_batch_cleaned.select("city_clean", "latitude", "longitude") \
    .union(df_stream_cleaned.select("city_clean", "latitude", "longitude")) \
    .filter("city_clean IS NOT NULL AND city_clean != ''") \
    .groupBy("city_clean") \
    .agg(
        F.avg("latitude").alias("lat"), 
        F.avg("longitude").alias("lon")
    )

print(f"📊 Actual number of cities after cleaning: {df_unique_cities.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import time
from pyspark.sql import Row

locations = df_unique_cities.collect()
weather_results = []
failed_locations = [] # 👈 We will collect the error areas here.
counter = 0

def fetch_weather(city, lat, lon):
    url = f"https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lon}&start_date=2023-11-01&end_date=2025-10-31&daily=temperature_2m_max,rain_sum&timezone=auto"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            return response.json().get('daily', {})
        return None
    except:
        return None

# --- 1st ROUND: MAIN DRAW ---
print(f"🚀 The first round is starting for {len(locations)} cities....")
for loc in locations:
    city, lat, lon = loc['city_clean'], loc['lat'], loc['lon']
    counter += 1
    
    data = fetch_weather(city, lat, lon)
    if data and 'time' in data:
        for i in range(len(data['time'])):
            weather_results.append(Row(weather_date=data['time'][i], city_name=city, 
                                     temp_max=float(data['temperature_2m_max'][i] or 0.0), 
                                     rain_sum=float(data['rain_sum'][i] or 0.0)))
        print(f"[{counter}/{len(locations)}] ✅ {city} was withdrawn.", end="\r")
    else:
        failed_locations.append(loc) # 👈 Add the error field here
        print(f"\n❌ {city} has been skipped and added to the list.")

# --- ROUND 2: TRY AGAIN WITH THOSE WHO WERE SKIPPED ---
if failed_locations:
    print(f"\n\n🔄 The first round is over. The skipped {len(failed_locations)} cities are being retried....")
    time.sleep(5) # Rest the API
    
    for loc in failed_locations:
        city, lat, lon = loc['city_clean'], loc['lat'], loc['lon']
        data = fetch_weather(city, lat, lon)
        if data and 'time' in data:
            for i in range(len(data['time'])):
                weather_results.append(Row(weather_date=data['time'][i], city_name=city, 
                                         temp_max=float(data['temperature_2m_max'][i] or 0.0), 
                                         rain_sum=float(data['rain_sum'][i] or 0.0)))
            print(f"♻️ {city} was successfully filmed this time.")
        else:
            print(f"💀 {city} failed on the second attempt too.")

# 2. Save
df_weather_final = spark.createDataFrame(weather_results)
df_weather_final.write.format("delta").mode("overwrite").saveAsTable("bronze_api_weather")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Current cities served
downloaded_cities = spark.table("bronze_api_weather").select("city_name").distinct()

# Those not yet withdrawn (Left Anti Join)
df_missing_cities = df_unique_cities.join(
    downloaded_cities, 
    df_unique_cities.city_clean == downloaded_cities.city_name, 
    "left_anti"
)

missing_locations = df_missing_cities.collect()
print(f"🔍 Number of cities still missing: {len(missing_locations)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import time
from pyspark.sql import Row

weather_results_retry = []

print(f"🔄 The second operation is commencing for the remaining {len(missing_locations)} cities...")

for loc in missing_locations:
    city, lat, lon = loc['city_clean'], loc['lat'], loc['lon']
    
    url = f"https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lon}&start_date=2023-11-01&end_date=2025-10-31&daily=temperature_2m_max,rain_sum&timezone=auto"
    
    try:
        # Daha sabırlı bir timeout (20 sn)
        response = requests.get(url, timeout=20)
        
        if response.status_code == 200:
            data = response.json().get('daily', {})
            if data and 'time' in data:
                for i in range(len(data['time'])):
                    weather_results_retry.append(Row(
                        weather_date=data['time'][i],
                        city_name=city,
                        temp_max=float(data['temperature_2m_max'][i] or 0.0),
                        rain_sum=float(data['rain_sum'][i] or 0.0)
                    ))
                print(f"✅ Rescued: {city}")
                time.sleep(2) # To avoid overloading the API, wait after each successful retrieval.
        
        elif response.status_code == 429:
            print(f"🛑 The limit is still active, 30-second break... ({city})")
            time.sleep(30)
            
    except Exception as e:
        print(f"⚠️ {city} failed again: {str(e)}")

# 3. Mevcut tabloya ekle (MODE="APPEND")
if weather_results_retry:
    df_retry = spark.createDataFrame(weather_results_retry)
    df_retry.write.format("delta").mode("append").saveAsTable("bronze_api_weather")
    print(f"✨ Missing cities have been successfully added!")
else:
    print("❌ New data could not be retrieved.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
