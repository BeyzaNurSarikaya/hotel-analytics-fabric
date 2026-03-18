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

# # 01_bronze_ingest_batch

# CELL ********************

from pyspark.sql.functions import col, current_timestamp, input_file_name, lit
from pyspark.sql.types import StringType

# 1. PARAMETERS
source_path = "Files/hotel_raw_batch.csv" 
bronze_table_name = "bronze_hotel_batch"

# 2. CSV READING (with Advanced Settings)
df_raw = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("sep", ",") \
    .option("quote", '"') \
    .option("escape", '"') \
    .option("multiLine", "true") \
    .load(source_path)


print(f"Number of Raw Lines Read: {df_raw.count()}")

# 3. ADDING METADATA
# Data cannot be altered at the Bronze tier, but information about "when" and "from which file" it came is added.
# This allows us to trace back to the source in case of future errors.
df_bronze = df_raw.withColumn("ingestion_timestamp", current_timestamp()) \
                  .withColumn("source_file", input_file_name())

# 4. WRITING IN DELTA FORMAT
# As there are over 1 million rows, we create a Delta table in overwrite mode.
# The Delta format supports ACID transactions, time travel, and fast queries.
df_bronze.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(bronze_table_name)

print(f"✅ Successful: The {bronze_table_name} table has been created.")

# 5. CONTROL
# Let's see the first 5 rows and the total count from the table
display(spark.sql(f"SELECT count(*) as total_rows FROM {bronze_table_name}"))
display(spark.sql(f"SELECT * FROM {bronze_table_name} LIMIT 5"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
