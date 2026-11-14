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

# CELL ********************

from pyspark.sql import functions as F
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

# Load Silver tables
silver_hourly = spark.read.table(
    "lh_silver.dbo.silver_air_quality_hourly"
)

silver_sensors = spark.read.table(
    "lh_silver.dbo.silver_air_quality_sensor"
)

print(f"Hourly records: {silver_hourly.count():,}")
print(f"Sensor records: {silver_sensors.count():,}")

# Check parameter and unit values
display(
    silver_hourly
    .groupBy("parameter", "unit")
    .count()
)

# Check flagged vs unflagged observations
display(
    silver_hourly
    .groupBy("has_flags")
    .count()
)

# Check measurements by sensor
display(
    silver_hourly
    .groupBy("sensor_id")
    .agg(
        F.count("*").alias("total_records"),
        F.countDistinct("local_date").alias("days_available"),
        F.sum(
            F.when(F.col("has_flags") == True, 1).otherwise(0)
        ).alias("flagged_records")
    )
    .orderBy("sensor_id")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Missing or invalid required fields
invalid_records = silver_hourly.filter(
    F.col("sensor_id").isNull()
    | F.col("timestamp_utc").isNull()
    | F.col("local_date").isNull()
    | F.col("pm25_value").isNull()
    | (F.col("pm25_value") < 0)
)

invalid_count = invalid_records.count()

# Unexpected parameter or unit
unexpected_records = silver_hourly.filter(
    F.col("parameter").isNull()
    | (F.lower(F.trim(F.col("parameter"))) != "pm25")
    | F.col("unit").isNull()
    | (F.trim(F.col("unit")) != "µg/m³")
)

unexpected_count = unexpected_records.count()

# Duplicate sensor-hour combinations
duplicate_records = (
    silver_hourly
    .groupBy("sensor_id", "timestamp_utc")
    .count()
    .filter(F.col("count") > 1)
)

duplicate_count = duplicate_records.count()

print(f"Invalid records: {invalid_count}")
print(f"Unexpected parameter/unit records: {unexpected_count}")
print(f"Duplicate sensor-hour combinations: {duplicate_count}")

if invalid_count > 0 or unexpected_count > 0 or duplicate_count > 0:
    raise ValueError(
        "Silver hourly data failed validation. "
        "Investigate the records before Gold aggregation."
    )
daily_metrics = (
    silver_hourly
    .groupBy("sensor_id", "local_date")
    .agg(
        # Number of valid hourly observations
        F.countDistinct("timestamp_utc")
        .alias("valid_hour_count"),

        # Flagged and unflagged hourly observations
        F.countDistinct(
            F.when(
                F.coalesce(F.col("has_flags"), F.lit(False)),
                F.col("timestamp_utc")
            )
        ).alias("flagged_hour_count"),

        F.countDistinct(
            F.when(
                ~F.coalesce(F.col("has_flags"), F.lit(False)),
                F.col("timestamp_utc")
            )
        ).alias("unflagged_hour_count"),

        # Statistics using all valid observations
        F.avg("pm25_value").alias("avg_pm25_all_valid"),
        F.min("pm25_value").alias("min_pm25_all_valid"),
        F.max("pm25_value").alias("max_pm25_all_valid"),

        # Statistics excluding flagged observations
        F.avg(
            F.when(
                ~F.coalesce(F.col("has_flags"), F.lit(False)),
                F.col("pm25_value")
            )
        ).alias("avg_pm25_unflagged"),

        F.min(
            F.when(
                ~F.coalesce(F.col("has_flags"), F.lit(False)),
                F.col("pm25_value")
            )
        ).alias("min_pm25_unflagged"),

        F.max(
            F.when(
                ~F.coalesce(F.col("has_flags"), F.lit(False)),
                F.col("pm25_value")
            )
        ).alias("max_pm25_unflagged"),

        # Average coverage within each source hour
        F.avg("source_hour_coverage_percent")
        .alias("avg_source_hour_coverage_pct")
    )
)

display(daily_metrics.orderBy("local_date", "sensor_id"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Determine the actual date range in Silver
date_bounds = silver_hourly.agg(
    F.min("local_date").alias("min_date"),
    F.max("local_date").alias("max_date")
).first()

min_date = date_bounds["min_date"]
max_date = date_bounds["max_date"]

if min_date is None or max_date is None:
    raise ValueError("No local dates found in Silver hourly data.")

# Generate every calendar date in the range
ny_timezone = ZoneInfo("America/New_York")
utc_timezone = timezone.utc

expected_hours = []

current_date = min_date

while current_date <= max_date:
    start_local = datetime.combine(
        current_date,
        time.min,
        tzinfo=ny_timezone
    )

    end_local = datetime.combine(
        current_date + timedelta(days=1),
        time.min,
        tzinfo=ny_timezone
    )

    hours = int(
        (
            end_local.astimezone(utc_timezone)
            - start_local.astimezone(utc_timezone)
        ).total_seconds() / 3600
    )

    expected_hours.append((current_date, hours))
    current_date += timedelta(days=1)

expected_hours_df = spark.createDataFrame(
    expected_hours,
    ["local_date", "expected_hour_count"]
)

display(
    expected_hours_df
    .filter(F.col("expected_hour_count") != 24)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# One row per sensor
sensor_ids = (
    silver_sensors
    .select("sensor_id")
    .distinct()
)

# Every sensor paired with every local date
daily_grid = sensor_ids.crossJoin(expected_hours_df)

# Attach the daily measurements
fact_air_quality_daily_sensor = (
    daily_grid
    .join(
        daily_metrics,
        on=["sensor_id", "local_date"],
        how="left"
    )
    .fillna({
        "valid_hour_count": 0,
        "flagged_hour_count": 0,
        "unflagged_hour_count": 0
    })
    .withColumn(
        "hour_coverage_pct",
        F.round(
            F.col("valid_hour_count") * 100.0
            / F.col("expected_hour_count"),
            2
        )
    )
    .withColumn(
        "unflagged_hour_coverage_pct",
        F.round(
            F.col("unflagged_hour_count") * 100.0
            / F.col("expected_hour_count"),
            2
        )
    )
)

display(
    fact_air_quality_daily_sensor
    .orderBy("local_date", "sensor_id")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    fact_air_quality_daily_sensor
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("dbo.fact_air_quality_daily_sensor")
)

print("fact_air_quality_daily_sensor created successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    silver_hourly
    .groupBy("sensor_id", "has_flags")
    .count()
    .orderBy("sensor_id", "has_flags")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_flags = spark.read.table(
    "lh_silver.dbo.silver_air_quality_flags"
)

silver_flags.printSchema()

print(f"Total flag records: {silver_flags.count()}")

display(silver_flags.limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_quarantine = spark.read.table(
    "lh_silver.dbo.silver_air_quality_quarantine"
)

silver_quarantine.printSchema()

print(
    f"Total quarantined records: "
    f"{silver_quarantine.count():,}"
)

display(silver_quarantine.limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    silver_quarantine
    .groupBy("sensor_id", "quality_status")
    .count()
    .orderBy("sensor_id", "quality_status")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
display(
    silver_quarantine
    .filter(F.col("pm25_value") < 0)
    .groupBy("sensor_id")
    .agg(
        F.count("*").alias("negative_measurements"),
        F.min("pm25_value").alias("minimum_pm25"),
        F.max("pm25_value").alias("maximum_pm25")
    )
    .orderBy("sensor_id")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    silver_quarantine
    .groupBy("quality_status", "has_flags")
    .count()
    .orderBy("quality_status", "has_flags")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    silver_quarantine
    .groupBy("sensor_id", "local_date", "quality_status")
    .count()
    .orderBy("local_date", "sensor_id")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_hourly = spark.read.table(
    "lh_silver.dbo.silver_air_quality_hourly"
)

display(silver_hourly.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    silver_hourly
    .groupBy("sensor_id", "has_flags")
    .count()
    .orderBy("sensor_id", "has_flags")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
