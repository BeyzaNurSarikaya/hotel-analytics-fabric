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

# CELL ********************

df = spark.read.table("bronze_hotel_stream")
print(df.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_batch = spark.read.table("bronze_hotel_batch")
print(df_batch.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.table("gold_hotel_weather_analytics")
df.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.table("bronze_hotel_stream")
df.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pyspark.sql.functions as F

df = spark.read.table("bronze_api_weather")

result_df = (
    df.groupBy("city_name")
      .agg(F.count("*").alias("city_count"))
      .orderBy(F.col("city_count").desc())
)

result_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.table("bronze_api_weather")

distinct_cities = df.select("city_name").distinct()

distinct_cities.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

distinct_cities.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
