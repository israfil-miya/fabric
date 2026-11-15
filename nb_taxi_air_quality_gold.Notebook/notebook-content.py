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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Cell 1: First inspect the existing schemas

from pyspark.sql import functions as F

taxi_daily = spark.read.table(
    "dbo.fact_taxi_daily"
)

air_quality_daily = spark.read.table(
    "dbo.fact_air_quality_daily_citywide"
)

dim_date = spark.read.table(
    "dbo.dim_date"
)

print("TAXI FACT")
taxi_daily.printSchema()

print("AIR QUALITY FACT")
air_quality_daily.printSchema()

print("DATE DIMENSION")
dim_date.printSchema()

print("TAXI SAMPLE")
display(taxi_daily.limit(5))

print("AIR QUALITY SAMPLE")
display(air_quality_daily.limit(5))

print("DATE DIMENSION SAMPLE")
display(dim_date.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 2: Build the combined taxi and air-quality fact

from pyspark.sql import functions as F

# Read Gold source tables
taxi_daily = spark.table("dbo.fact_taxi_daily")
air_quality_daily = spark.table("dbo.fact_air_quality_daily_citywide")
dim_date = spark.table("dbo.dim_date")

# Add the shared date key to the air-quality fact
air_quality_with_key = (
    air_quality_daily.alias("aq")
    .join(
        dim_date.select("date_key", "full_date").alias("d"),
        F.col("aq.local_date") == F.col("d.full_date"),
        "inner"
    )
    .select(
        "d.date_key",
        "aq.available_sensor_count",
        "aq.eligible_sensor_count",
        "aq.min_sensor_avg_pm25",
        "aq.max_sensor_avg_pm25",
        "aq.avg_eligible_sensor_coverage_pct",
        "aq.network_mean_pm25",
        "aq.network_median_pm25",
        "aq.minimum_sensor_coverage_pct",
        "aq.minimum_eligible_sensor_count"
    )
)

# Preserve all taxi dates
fact_taxi_air_quality = (
    taxi_daily
    .join(
        air_quality_with_key,
        on="date_key",
        how="left"
    )
)

# Write the combined fact to lh_gold
(
    fact_taxi_air_quality.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("dbo.fact_taxi_air_quality_daily")
)

print("Successfully created fact_taxi_air_quality_daily")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 3: Validate the combined Gold fact

taxi = spark.table("dbo.fact_taxi_daily")
combined = spark.table("dbo.fact_taxi_air_quality_daily")

taxi_count = taxi.count()
combined_count = combined.count()

duplicate_dates = (
    combined.groupBy("date_key")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print("Taxi fact rows:", taxi_count)
print("Combined fact rows:", combined_count)
print("Duplicate date keys:", duplicate_dates)

combined.agg(
    F.min("date_key").alias("first_date"),
    F.max("date_key").alias("last_date"),
    F.count("network_mean_pm25").alias("dates_with_pm25"),
    F.count("*").alias("total_dates")
).show()

assert taxi_count == combined_count, "Row count mismatch"
assert duplicate_dates == 0, "Duplicate date keys found"

print("All validation checks passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
