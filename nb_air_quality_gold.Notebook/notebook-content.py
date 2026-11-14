# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9d65ce6c-ba84-4650-9668-115a27ff14a7",
# META       "default_lakehouse_name": "lh_gold",
# META       "default_lakehouse_workspace_id": "14530c94-9dc4-4ef1-a320-081555064f5d",
# META       "known_lakehouses": [
# META         {
# META           "id": "9d65ce6c-ba84-4650-9668-115a27ff14a7"
# META         },
# META         {
# META           "id": "29b14a15-b502-4b45-8d4e-a0b8ee212b0f"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F

# Read sensor metadata from the Silver Lakehouse
silver_sensor = spark.read.table(
    "lh_silver.dbo.silver_air_quality_sensor"
)

display(silver_sensor)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_sensor.printSchema()

print(f"Total sensor records: {silver_sensor.count()}")
print(f"Total columns: {len(silver_sensor.columns)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check for null sensor IDs
null_sensor_ids = silver_sensor.filter(
    F.col("sensor_id").isNull()
).count()

# Check for duplicate sensor IDs
duplicate_sensor_ids = (
    silver_sensor
    .groupBy("sensor_id")
    .count()
    .filter(F.col("count") > 1)
)

duplicate_count = duplicate_sensor_ids.count()

print(f"Null sensor IDs: {null_sensor_ids}")
print(f"Duplicate sensor IDs: {duplicate_count}")

if null_sensor_ids > 0 or duplicate_count > 0:
    raise ValueError(
        "Sensor metadata validation failed. "
        "Resolve null or duplicate sensor IDs before proceeding."
    )

print("Sensor metadata validation passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_air_quality_sensor = (
    silver_sensor
    .select("*")
    .orderBy("sensor_id")
)

display(dim_air_quality_sensor)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    dim_air_quality_sensor
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("dbo.dim_air_quality_sensor")
)

print("dim_air_quality_sensor created successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_sensor = spark.read.table(
    "dbo.dim_air_quality_sensor"
)

print(f"Gold sensor records: {gold_sensor.count()}")

display(
    gold_sensor.orderBy("sensor_id")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
