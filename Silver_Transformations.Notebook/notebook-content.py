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

# # Silver Transformations

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import *

# 1. SELECT AND COMBINE COMMON COLUMNS
batch_cols = set(spark.table("bronze_hotel_batch").columns)
stream_cols = set(spark.table("bronze_hotel_stream").columns)
common_cols = list(batch_cols.intersection(stream_cols))

df_raw = spark.table("bronze_hotel_batch").select(*common_cols) \
    .union(spark.table("bronze_hotel_stream").select(*common_cols))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# List of columns to be cleaned 
columns_to_clean = [
    'hotel_id', 'hotel_name','country', 'city', 'hotel_type', 'hotel_facilities', 
    'nearby_attractions', 'website', 'customer_id', 'first_name', 
    'last_name', 'full_name', 'email', 'phone', 'birth_date', 'gender', 
    'country_customer', 'city_customer', 'address', 'postal_code', 
    'language_preference', 'loyalty_level_customer', 'registration_date', 
    'marketing_consent', 'booking_date', 'checkin_date', 'checkout_date', 
    'room_type', 'booking_channel', 'special_requests', 'is_cancelled', 
    'cancellation_date', 'cancellation_reason', 'payment_status', 
    'payment_method', 'booking_source', 'promotion_code', 'created_timestamp', 
    'updated_timestamp', 'created_by', 'booking_status', 'review_date', 
    'is_verified_review', 'reviewer_location', 'stay_date', 'trip_type', 
    'room_type_reviewed', 'source_system', 'created_at', 'updated_at'
]

# Regular expression pattern containing the characters to be cleaned:
# ^[!|?|_|-]+  -> Finds the characters at the beginning
# [!|?|_|-]+$  -> Finds the characters at the end
# We also remove the spaces on the right/left using trim().
cleanup_pattern = r"^[!|?|_|\-|#|.]+|[!|?|_|\-|#|.]+$"

df_cleaned = df_raw

for col_name in columns_to_clean:
    df_cleaned = df_cleaned.withColumn(
        col_name, 
        F.trim(F.regexp_replace(F.col(col_name), cleanup_pattern, ""))
    )

# Let's check the result
df_cleaned.select("hotel_id", "country", "first_name").show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## hotel_id

# CELL ********************

# We are filtering rows where the # hotel_id column is NULL (None) or an empty string ("").
# Since some may become empty strings after regex cleaning, checking both is the safest approach.

df_cleaned = df_cleaned.filter(
    (F.col("hotel_id").isNotNull()) & 
    (F.col("hotel_id") != "") &
    (F.col("hotel_id") != "null") # Some systems may output 'null' as a string.
)

# Let's do a count check to see how many lines we have deleted
print(f"Number of lines remaining after cleaning: {df_cleaned.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Unknown

# CELL ********************

# List of columns to be labelled "Unknown"
cols_to_fill_unknown = [
    'hotel_name', 'hotel_type', 'hotel_facilities', 'nearby_attractions', 
    'website', 'email', 'gender', 'address', 'postal_code', 
    'loyalty_level_customer', 'room_type', 'booking_channel', 
    'payment_status', 'payment_method', 'booking_source', 
    'reviewer_location', 'trip_type', 'room_type_reviewed', 'source_system'
]

# First, let's convert empty strings ("") to the actual NULL value, 
# then use fillna to make them all "Unknown".
for col_name in cols_to_fill_unknown:
    df_cleaned = df_cleaned.withColumn(
        col_name,
        F.when((F.col(col_name) == "") | (F.col(col_name).isNull()), "Unknown")
        .otherwise(F.col(col_name))
    )

# Result verification (using a sample column)
df_cleaned.select("hotel_name", "booking_channel", "payment_method").show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # city, country, country_customer, city_customer

# CELL ********************

from pyspark.sql import Window

# Step 1: Retrieve City and Country from Hotel_name
# Group rows with the same hotel name and take the first non-null value
window_hotel = Window.partitionBy("hotel_name").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

df_cleaned = df_cleaned.withColumn(
    "city", 
    F.coalesce(F.col("city"), F.first("city", ignorenulls=True).over(window_hotel))
).withColumn(
    "country", 
    F.coalesce(F.col("country"), F.first("country", ignorenulls=True).over(window_hotel))
)

# Step 2: Rescue the country via the city
# If the city is known but the country is empty, find the country from other records with the same city
window_city = Window.partitionBy("city").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

df_cleaned = df_cleaned.withColumn(
    "country", 
    F.coalesce(F.col("country"), F.first("country", ignorenulls=True).over(window_city))
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Read the Reference Files 
df_city_country_ref = spark.read.csv("Files/City_Country.csv", header=True, inferSchema=True)
df_country_capital_ref = spark.read.csv("Files/country_capital.csv", header=True, inferSchema=True)

# Standardise reference tables (capitalisation and formatting)
df_city_country_ref = df_city_country_ref.withColumn("city", F.upper(F.trim(F.col("city")))) \
                                         .withColumn("country", F.upper(F.trim(F.col("country"))))

df_country_capital_ref = df_country_capital_ref.withColumn("country", F.upper(F.trim(F.col("country")))) \
                                               .withColumn("capital", F.upper(F.trim(F.col("capital"))))

# --- STEP 1: Finding a Country Based on a City (For Both the Hotel and the Customer) ---

# To avoid joining the reference table twice, we can do this in a single step by using an alias (nickname)
# Join for the customer’s city
df_cleaned = df_cleaned.join(
    df_city_country_ref.select(F.col("city").alias("ref_city_c"), F.col("country").alias("ref_country_c")),
    df_cleaned.city_customer == F.col("ref_city_c"),
    "left"
).join(
    df_city_country_ref.select(F.col("city").alias("ref_city_h"), F.col("country").alias("ref_country_h")),
    df_cleaned.city == F.col("ref_city_h"),
    "left"
)

# Fill if empty
df_cleaned = df_cleaned.withColumn(
    "country", 
    F.when((F.col("country").isNull()) | (F.col("country") == "UNKNOWN"), F.col("ref_country_h")).otherwise(F.col("country"))
).withColumn(
    "country_customer", 
    F.when((F.col("country_customer").isNull()) | (F.col("country_customer") == "UNKNOWN"), F.col("ref_country_c")).otherwise(F.col("country_customer"))
).drop("ref_city_c", "ref_country_c", "ref_city_h", "ref_country_h")


# --- STEP 2: Finding a City (Capital) by Country (For Both Hotels and Customers) ---

df_cleaned = df_cleaned.join(
    df_country_capital_ref.select(F.col("country").alias("ref_country_c"), F.col("capital").alias("ref_capital_c")),
    df_cleaned.country_customer == F.col("ref_country_c"),
    "left"
).join(
    df_country_capital_ref.select(F.col("country").alias("ref_country_h"), F.col("capital").alias("ref_capital_h")),
    df_cleaned.country == F.col("ref_country_h"),
    "left"
)

# Fill if empty
df_cleaned = df_cleaned.withColumn(
    "city", 
    F.when((F.col("city").isNull()) | (F.col("city") == "UNKNOWN"), F.col("ref_capital_h")).otherwise(F.col("city"))
).withColumn(
    "city_customer", 
    F.when((F.col("city_customer").isNull()) | (F.col("city_customer") == "UNKNOWN"), F.col("ref_capital_c")).otherwise(F.col("city_customer"))
).drop("ref_country_c", "ref_capital_c", "ref_country_h", "ref_capital_h")

# --- STEP 3: Final Touches (Final UNKNOWN and Capital Letters) ---
all_geo_cols = ["country", "city", "country_customer", "city_customer"]

for col_name in all_geo_cols:
    df_cleaned = df_cleaned.withColumn(
        col_name,
        F.when((F.col(col_name).isNull()) | (F.col(col_name) == ""), "UNKNOWN").otherwise(F.upper(F.col(col_name)))
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Dates

# CELL ********************

from pyspark.sql import functions as F

date_columns = [
    'birth_date', 'booking_date', 'checkin_date', 'checkout_date', 
    'cancellation_date', 'review_date', 'stay_date'
]

formats = ["yyyy-MM-dd", "MM/dd/yyyy", "dd/MM/yyyy", "yyyy/MM/dd"]
for col_name in date_columns:
    df_cleaned = df_cleaned.withColumn(
        col_name,
        F.coalesce(*[F.to_date(F.col(col_name), fmt) for fmt in formats])
    )


df_cleaned = df_cleaned.withColumn(
    "booking_date",
    F.when(
        F.col("booking_date").isNull(), 
        F.col("checkin_date")
    ).otherwise(F.col("booking_date"))
)


for col_name in ['booking_date', 'checkin_date']:
    df_cleaned = df_cleaned.withColumn(
        col_name,
        F.when(F.year(F.col(col_name)) > 2025, F.lit(None)).otherwise(F.col(col_name))
    )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_cleaned.select(F.year("booking_date").alias("year")) \
    .groupBy("year") \
    .count() \
    .orderBy("year") \
    .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # star_rating, review_rating

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# Conversion glossary
rating_map = {
    "one": "1", "two": "2", "three": "3", "four": "4", "five": "5",
    "six": "5", "seven": "5", "eight": "5", "nine": "5", "ten": "5"
}

def finalize_ratings(df, col_name, max_val):
    # 1. First, string cleaning (lowercase and trim)
    temp_col = F.lower(F.trim(F.col(col_name)))
    
    # 2. Convert the spoken numbers into digits
    for word, num in rating_map.items():
        temp_col = F.regexp_replace(temp_col, word, num)
    
    # 3. Convert to integer (spaces and meaningless text are automatically converted to NULL)
    df = df.withColumn(col_name, temp_col.cast(IntegerType()))
    
    # 4. Logical filtering (e.g. filter out those exceeding the 5-star limit or those below 0)
    df = df.withColumn(
        col_name,
        F.when((F.col(col_name) < 1) | (F.col(col_name) > max_val), F.lit(None))
        .otherwise(F.col(col_name))
    )
    return df



df_cleaned = finalize_ratings(df_cleaned, "star_rating", 5)


df_cleaned = finalize_ratings(df_cleaned, "review_rating", 5)

df_cleaned.select("star_rating", "review_rating").show(20)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # total_rooms

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# 1. We clean up the string and raw values and cast them to Integer

df_cleaned = df_cleaned.withColumn(
    "total_rooms", 
    F.col("total_rooms").cast(IntegerType())
)

# 2. Logical Filtering
# We set negative values, 0 and extreme values (9999) to NULL (lit(None))
df_cleaned = df_cleaned.withColumn(
    "total_rooms",
    F.when(
        (F.col("total_rooms") <= 0) | (F.col("total_rooms") >= 9999), 
        F.lit(None)
    ).otherwise(F.col("total_rooms"))
)



# Check: Have the outliers been removed?
df_cleaned.select("total_rooms").orderBy(F.desc("total_rooms")).show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_cleaned.select("total_rooms").show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Money Columns

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# List of financial columns
money_columns = [
    'total_price', 'room_price', 'tax_amount', 
    'service_fee', 'paid_amount', 'discount_amount'
]

for col_name in money_columns:
    # 1. Clean up text values and remove any unwanted characters, then convert them to numbers
    # (DoubleType is suitable for prices with decimal places)
    df_cleaned = df_cleaned.withColumn(
        col_name, 
        F.col(col_name).cast(DoubleType())
    )
    
    # 2. Set negative values to NULL (lit(None))
    # Note: The `discount_amount` may sometimes be stored as a negative value in some systems (e.g. a 10% discount), 
    # but here we are keeping them all as positive values.
    df_cleaned = df_cleaned.withColumn(
        col_name,
        F.when(F.col(col_name) < 0, F.lit(None)).otherwise(F.col(col_name))
    )

# Optional: if discount_amount or tax_amount is null, we can treat it as 0 
# (Because there may be no discount or tax, but the room rate must always be present)
df_cleaned = df_cleaned.fillna(0, subset=['tax_amount', 'service_fee', 'discount_amount'])

# Check: Are there any negative values left?
df_cleaned.select(money_columns).describe().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Upper ID Columns

# CELL ********************

columns_to_id = [
    'hotel_id', 'customer_id'
]

for col_name in columns_to_id:
    df_cleaned = df_cleaned.withColumn(
            col_name, 
            F.upper(F.col(col_name))
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Save to Silver

# CELL ********************

# Saving as a Delta table
# The 'overwrite' mode overwrites the table if it exists; 'append' appends to the end.
# As this is the Silver tier, overwriting is generally preferred.

target_table_name = "silver_hotel_cleaned"

df_cleaned.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(target_table_name)

print(f"The data has been successfully saved in Delta format as '{target_table_name}'.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
