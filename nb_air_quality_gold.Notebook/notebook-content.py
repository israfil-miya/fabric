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

# Cell 1: Imports and load Silver source tables

from pyspark.sql import functions as F
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

silver_sensor = spark.read.table(
    "lh_silver.dbo.silver_air_quality_sensor"
)

silver_hourly = spark.read.table(
    "lh_silver.dbo.silver_air_quality_hourly"
)

silver_quarantine = spark.read.table(
    "lh_silver.dbo.silver_air_quality_quarantine"
)

print(f"Silver sensors: {silver_sensor.count():,}")
print(f"Accepted hourly records: {silver_hourly.count():,}")
print(f"Quarantined records: {silver_quarantine.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 2: Validate source data

# Sensor metadata validation
null_sensor_ids = silver_sensor.filter(
    F.col("sensor_id").isNull()
).count()

duplicate_sensor_ids = (
    silver_sensor
    .groupBy("sensor_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

# Accepted hourly records validation
invalid_hourly_records = silver_hourly.filter(
    F.col("sensor_id").isNull()
    | F.col("timestamp_utc").isNull()
    | F.col("local_date").isNull()
    | F.col("pm25_value").isNull()
    | (F.col("pm25_value") < 0)
    | F.col("parameter").isNull()
    | (F.lower(F.trim(F.col("parameter"))) != "pm25")
    | F.col("unit").isNull()
    | (F.trim(F.col("unit")) != "µg/m³")
    | F.col("has_flags").isNull()
).count()

duplicate_hourly_records = (
    silver_hourly
    .groupBy("sensor_id", "timestamp_utc")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

# Accepted records should not contain flagged observations
accepted_flagged_records = silver_hourly.filter(
    F.col("has_flags") == True
).count()

# Quarantine validation
invalid_quarantine_records = silver_quarantine.filter(
    F.col("sensor_id").isNull()
    | F.col("timestamp_utc").isNull()
    | F.col("local_date").isNull()
    | F.col("quality_status").isNull()
    | F.col("has_flags").isNull()
).count()

duplicate_quarantine_records = (
    silver_quarantine
    .groupBy("sensor_id", "timestamp_utc")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

unexpected_quarantine_statuses = (
    silver_quarantine
    .filter(
        ~F.col("quality_status").isin(
            "FLAGGED",
            "MISSING_VALUE"
        )
    )
    .count()
)

# Accepted and quarantined records should not overlap
overlapping_records = (
    silver_hourly
    .select("sensor_id", "timestamp_utc")
    .join(
        silver_quarantine.select("sensor_id", "timestamp_utc"),
        on=["sensor_id", "timestamp_utc"],
        how="inner"
    )
    .count()
)

validation_results = {
    "Null sensor IDs": null_sensor_ids,
    "Duplicate sensor IDs": duplicate_sensor_ids,
    "Invalid accepted records": invalid_hourly_records,
    "Duplicate accepted sensor-hours": duplicate_hourly_records,
    "Flagged accepted records": accepted_flagged_records,
    "Invalid quarantined records": invalid_quarantine_records,
    "Duplicate quarantined sensor-hours": duplicate_quarantine_records,
    "Unexpected quarantine statuses": unexpected_quarantine_statuses,
    "Overlapping accepted/quarantined records": overlapping_records
}

for name, count in validation_results.items():
    print(f"{name}: {count:,}")

if any(validation_results.values()):
    raise ValueError(
        "Source validation failed. "
        "Investigate the reported issues before proceeding."
    )

print("All source validations passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 3: Create sensor dimension

dim_air_quality_sensor = silver_sensor

(
    dim_air_quality_sensor
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("dbo.dim_air_quality_sensor")
)

print(
    f"dim_air_quality_sensor created successfully: "
    f"{dim_air_quality_sensor.count():,} records"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 4: Aggregate accepted hourly measurements

valid_daily = (
    silver_hourly
    .groupBy("sensor_id", "local_date")
    .agg(
        F.countDistinct("timestamp_utc")
        .alias("valid_hour_count"),

        F.countDistinct(
            F.when(
                F.col("has_flags") == False,
                F.col("timestamp_utc")
            )
        ).alias("unflagged_hour_count"),

        F.avg("pm25_value")
        .alias("avg_pm25_all_valid"),

        F.min("pm25_value")
        .alias("min_pm25_all_valid"),

        F.max("pm25_value")
        .alias("max_pm25_all_valid"),

        F.avg(
            F.when(
                F.col("has_flags") == False,
                F.col("pm25_value")
            )
        ).alias("avg_pm25_unflagged"),

        F.min(
            F.when(
                F.col("has_flags") == False,
                F.col("pm25_value")
            )
        ).alias("min_pm25_unflagged"),

        F.max(
            F.when(
                F.col("has_flags") == False,
                F.col("pm25_value")
            )
        ).alias("max_pm25_unflagged"),

        F.avg("source_hour_coverage_percent")
        .alias("avg_source_hour_coverage_pct")
    )
)

print(f"Accepted daily sensor groups: {valid_daily.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 5: Aggregate quarantine quality metrics

quarantine_daily = (
    silver_quarantine
    .groupBy("sensor_id", "local_date")
    .agg(
        F.countDistinct("timestamp_utc")
        .alias("quarantined_hour_count"),

        F.countDistinct(
            F.when(
                F.col("has_flags") == True,
                F.col("timestamp_utc")
            )
        ).alias("flagged_hour_count"),

        F.countDistinct(
            F.when(
                F.col("quality_status") == "FLAGGED",
                F.col("timestamp_utc")
            )
        ).alias("flagged_value_hour_count"),

        F.countDistinct(
            F.when(
                F.col("quality_status") == "MISSING_VALUE",
                F.col("timestamp_utc")
            )
        ).alias("missing_value_hour_count")
    )
)

print(
    f"Quarantined daily sensor groups: "
    f"{quarantine_daily.count():,}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 6: Build daily sensor fact

# Get the full date range from both Silver tables
all_dates = (
    silver_hourly
    .select("local_date")
    .unionByName(
        silver_quarantine.select("local_date")
    )
    .distinct()
)

date_bounds = all_dates.agg(
    F.min("local_date").alias("min_date"),
    F.max("local_date").alias("max_date")
).first()

min_date = date_bounds["min_date"]
max_date = date_bounds["max_date"]

if min_date is None or max_date is None:
    raise ValueError("No local dates found in Silver data.")

# Generate expected hours for each local date
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

# Create a complete sensor/date grid
sensor_ids = (
    dim_air_quality_sensor
    .select("sensor_id")
    .distinct()
)

daily_grid = sensor_ids.crossJoin(expected_hours_df)

# Join accepted and quarantined measurements
fact_air_quality_daily_sensor = (
    daily_grid
    .join(
        valid_daily,
        on=["sensor_id", "local_date"],
        how="left"
    )
    .join(
        quarantine_daily,
        on=["sensor_id", "local_date"],
        how="left"
    )
)

# Fill missing count values with zero
count_columns = [
    "valid_hour_count",
    "unflagged_hour_count",
    "quarantined_hour_count",
    "flagged_hour_count",
    "flagged_value_hour_count",
    "missing_value_hour_count"
]

for column in count_columns:
    fact_air_quality_daily_sensor = (
        fact_air_quality_daily_sensor
        .withColumn(
            column,
            F.coalesce(F.col(column), F.lit(0)).cast("long")
        )
    )

# Calculate coverage
fact_air_quality_daily_sensor = (
    fact_air_quality_daily_sensor
    .withColumn(
        "source_observed_hour_count",
        F.col("valid_hour_count")
        + F.col("quarantined_hour_count")
    )
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
    .withColumn(
        "source_observed_hour_coverage_pct",
        F.round(
            F.col("source_observed_hour_count") * 100.0
            / F.col("expected_hour_count"),
            2
        )
    )
)

print(
    f"Daily sensor fact prepared: "
    f"{fact_air_quality_daily_sensor.count():,} records"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 7: Save daily sensor fact

(
    fact_air_quality_daily_sensor
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("dbo.fact_air_quality_daily_sensor")
)

print("fact_air_quality_daily_sensor saved successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 8: Validate Gold tables

gold_sensor = spark.read.table(
    "dbo.dim_air_quality_sensor"
)

gold_daily = spark.read.table(
    "dbo.fact_air_quality_daily_sensor"
)

# Check expected number of sensor/date records
expected_record_count = (
    gold_sensor.select("sensor_id").distinct().count()
    * len(expected_hours)
)

actual_record_count = gold_daily.count()

if actual_record_count != expected_record_count:
    raise ValueError(
        f"Daily fact record count mismatch: "
        f"expected {expected_record_count}, "
        f"got {actual_record_count}."
    )

# Compare Gold totals against Silver source totals
gold_totals = gold_daily.agg(
    F.sum("valid_hour_count").alias("valid_hours"),
    F.sum("quarantined_hour_count").alias("quarantined_hours"),
    F.sum("flagged_hour_count").alias("flagged_hours"),
    F.sum("flagged_value_hour_count").alias("flagged_value_hours"),
    F.sum("missing_value_hour_count").alias("missing_value_hours")
).first()

expected_totals = {
    "valid_hours": silver_hourly.select(
        "sensor_id", "timestamp_utc"
    ).distinct().count(),

    "quarantined_hours": silver_quarantine.select(
        "sensor_id", "timestamp_utc"
    ).distinct().count(),

    "flagged_hours": silver_quarantine.filter(
        F.col("has_flags") == True
    ).select("sensor_id", "timestamp_utc").distinct().count(),

    "flagged_value_hours": silver_quarantine.filter(
        F.col("quality_status") == "FLAGGED"
    ).select("sensor_id", "timestamp_utc").distinct().count(),

    "missing_value_hours": silver_quarantine.filter(
        F.col("quality_status") == "MISSING_VALUE"
    ).select("sensor_id", "timestamp_utc").distinct().count()
}

for metric, expected in expected_totals.items():
    actual = gold_totals[metric]

    print(f"{metric}: Gold={actual:,}, Silver={expected:,}")

    if actual != expected:
        raise ValueError(
            f"{metric} mismatch: Gold={actual}, Silver={expected}"
        )

print("\nGold validation passed.")

# Overall table statistics
print(f"\nGold sensor records: {gold_sensor.count():,}")
print(f"Gold daily fact records: {actual_record_count:,}")

gold_daily.agg(
    F.countDistinct("sensor_id").alias("unique_sensors"),
    F.countDistinct("local_date").alias("unique_dates"),
    F.min("local_date").alias("first_date"),
    F.max("local_date").alias("last_date")
).show(truncate=False)

# Quality metrics by sensor
display(
    gold_daily
    .groupBy("sensor_id")
    .agg(
        F.sum("valid_hour_count").alias("valid_hours"),
        F.sum("quarantined_hour_count").alias("quarantined_hours"),
        F.sum("flagged_hour_count").alias("flagged_hours"),
        F.sum("flagged_value_hour_count").alias("flagged_value_hours"),
        F.sum("missing_value_hour_count").alias("missing_value_hours"),
        F.round(
            F.avg("hour_coverage_pct"), 2
        ).alias("avg_valid_coverage_pct"),
        F.round(
            F.avg("source_observed_hour_coverage_pct"), 2
        ).alias("avg_source_observed_coverage_pct")
    )
    .orderBy("sensor_id")
)

# Check DST dates
display(
    gold_daily
    .select("local_date", "expected_hour_count")
    .distinct()
    .filter(F.col("expected_hour_count") != 24)
    .orderBy("local_date")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
