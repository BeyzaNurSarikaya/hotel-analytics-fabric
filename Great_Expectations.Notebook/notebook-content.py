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
# META     "environment": {
# META       "environmentId": "7d98ba43-6487-9ff3-45b4-6461afa5df06",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Data Quality (Great Expectations)

# CELL ********************

import great_expectations as gx
import json
from datetime import datetime
from notebookutils import mssparkutils

# 1. Context and Data Source Settings
context = gx.get_context()

# We proceed by checking to ensure we don’t encounter an error if it has already been created
expectation_suite_name = "hotel_silver_quality_suite"
try:
    suite = context.suites.add(gx.ExpectationSuite(name=expectation_suite_name))
except:
    suite = context.suites.get(name=expectation_suite_name)

# Data Source Definition

try:
    data_source = context.data_sources.add_spark(name="hotel_data_source_v1")
except:
    data_source = context.data_sources.get(name="hotel_data_source_v1")

try:
    data_asset = data_source.add_dataframe_asset(name="silver_hotel_asset")
except:
    data_asset = data_source.get_asset(name="silver_hotel_asset")

batch_definition = data_asset.add_batch_definition_whole_dataframe("hotel_batch_def")

# 2. Let’s populate the test suite
# --- Test 1: The ID must be unique ---
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="booking_id"))

# --- Test 2: Required Fields (Null Check) ---
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="hotel_id"))


# --- Test 3: The price and number of nights must be positive ---
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="total_price", min_value=0))


# --- Test 4: Rating Limits (1-5) ---
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="star_rating", min_value=1, max_value=5))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="review_rating", min_value=1, max_value=5))


# 3. Validation: Definition and Implementation
validation_definition = gx.ValidationDefinition(
    data = batch_definition,
    suite = suite,
    name = "hotel_silver_validation",
)

# We read the data from the table and put it through a test
df_silver = spark.read.table("silver_hotel_cleaned")
results = validation_definition.run(batch_parameters = {"dataframe": df_silver})

# 4. Save Report as JSON (Artifact)
report_dict = results.to_json_dict()
report_json = json.dumps(report_dict, indent=4, ensure_ascii=False)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
report_folder = "Files/DataQualityReports"
report_filename = f"{report_folder}/GE_Report_Silver_Hotel_{timestamp}.json"

mssparkutils.fs.mkdirs(report_folder)
mssparkutils.fs.put(report_filename, report_json, overwrite=True)

# 5. Visual Summary Output
print("-" * 50)
print(f"📊 DATA QUALITY TEST RESULT: {'✅ SUCCESSFUL' if results.success else '❌ FAILED'}")
print(f"📈 Success Rate: %{report_dict['statistics']['success_percent']:.2f}")
print(f"📂 Report Path: {report_filename}")
print("-" * 50)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
