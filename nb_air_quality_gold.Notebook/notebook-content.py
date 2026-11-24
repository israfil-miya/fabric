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

# MARKDOWN ********************

# ## Configuration
# The pipeline passes the resolved month list as JSON. Set `MONTHS_JSON` when running interactively.

# PARAMETERS CELL ********************

import json
import re

MONTHS_JSON = "[]"


def parse_months(months_json):
    try:
        months = json.loads(months_json)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError("MONTHS_JSON must be a JSON array of YYYY-MM strings") from exc

    if not isinstance(months, list) or not months:
        raise ValueError("At least one month must be supplied in MONTHS_JSON")
    if any(not isinstance(month, str) or not re.fullmatch(
            r"[0-9]{4}-(0[1-9]|1[0-2])", month) for month in months):
        raise ValueError("Each month must be a string in YYYY-MM format")
    if len(months) != len(set(months)):
        raise ValueError("MONTHS_JSON must not contain duplicate months")
    return months


MONTHS = parse_months(MONTHS_JSON)
print("months to process:", MONTHS)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 1: Pre-flight audit check and load Silver source tables for MONTHS

from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, LongType, StructField, StructType

AUDIT_TABLE = "lh_silver.dbo.silver_air_quality_quality_audit"
SENSOR_TABLE = "lh_silver.dbo.silver_air_quality_sensor"
HOURLY_TABLE = "lh_silver.dbo.silver_air_quality_hourly"
QUARANTINE_TABLE = "lh_silver.dbo.silver_air_quality_quarantine"

if not spark.catalog.tableExists(AUDIT_TABLE):
    raise ValueError(f"Missing Silver audit table {AUDIT_TABLE}; refusing to publish Gold")

audit_df = spark.table(AUDIT_TABLE).filter(F.col("local_year_month").isin(MONTHS))
audit_by_month = {row["local_year_month"]: row for row in audit_df.collect()}
missing_audit = sorted(set(MONTHS) - set(audit_by_month))
failed_audit = sorted(
    ym for ym, row in audit_by_month.items() if not row["reconciliation_passed"]
)
if missing_audit or failed_audit:
    raise ValueError(
        f"Air quality Silver audit check failed. Missing months: {missing_audit}; "
        f"failed months: {failed_audit}. Refusing to publish Gold."
    )

silver_sensor = spark.read.table(SENSOR_TABLE)
silver_hourly = (
    spark.read.table(HOURLY_TABLE)
    .filter(F.date_format("local_date", "yyyy-MM").isin(MONTHS))
)
silver_quarantine = (
    spark.read.table(QUARANTINE_TABLE)
    .filter(F.date_format("local_date", "yyyy-MM").isin(MONTHS))
)

print(f"Silver sensors: {silver_sensor.count():,}")
print(f"Accepted hourly records in MONTHS: {silver_hourly.count():,}")
print(f"Quarantined records in MONTHS: {silver_quarantine.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 2: Validate Silver source data for MONTHS

null_sensor_ids = silver_sensor.filter(F.col("sensor_id").isNull()).count()
duplicate_sensor_ids = (
    silver_sensor.groupBy("sensor_id").count().filter(F.col("count") > 1).count()
)

invalid_hourly_records = silver_hourly.filter(
    F.col("sensor_id").isNull()
    | F.col("timestamp_utc").isNull()
    | F.col("local_date").isNull()
    | F.col("pm25_value").isNull()
    | F.isnan(F.col("pm25_value"))
    | (F.col("pm25_value") < 0)
    | F.col("parameter").isNull()
    | (F.lower(F.trim(F.col("parameter"))) != "pm25")
    | F.col("unit").isNull()
    | (F.trim(F.col("unit")) != "µg/m³")
    | F.col("has_flags").isNull()
    | (F.col("flag_status") != "CONFIRMED_UNFLAGGED")
).count()

duplicate_hourly_records = (
    silver_hourly.groupBy("sensor_id", "timestamp_utc").count().filter(F.col("count") > 1).count()
)

accepted_flagged_records = silver_hourly.filter(F.col("has_flags") != False).count()

invalid_quarantine_records = silver_quarantine.filter(
    F.col("sensor_id").isNull()
    | F.col("timestamp_utc").isNull()
    | F.col("local_date").isNull()
    | F.col("quality_status").isNull()
    | F.col("flag_status").isNull()
    | (~F.col("flag_status").isin("CONFIRMED_FLAGGED", "CONFIRMED_UNFLAGGED", "UNKNOWN_FLAG_STATUS"))
    | ((F.col("flag_status") == "UNKNOWN_FLAG_STATUS") & F.col("has_flags").isNotNull())
    | ((F.col("flag_status") != "UNKNOWN_FLAG_STATUS") & F.col("has_flags").isNull())
).count()

duplicate_quarantine_records = (
    silver_quarantine.groupBy("sensor_id", "timestamp_utc").count().filter(F.col("count") > 1).count()
)

expected_quarantine_statuses = (
    "FLAGGED",
    "UNKNOWN_FLAG_STATUS",
    "MISSING_VALUE",
    "NEGATIVE_VALUE",
    "UNEXPECTED_PARAMETER",
    "UNEXPECTED_UNIT",
    "UNEXPECTED_PERIOD",
)
unexpected_quarantine_statuses = (
    silver_quarantine.filter(~F.col("quality_status").isin(*expected_quarantine_statuses)).count()
)

overlapping_records = (
    silver_hourly.select("sensor_id", "timestamp_utc")
    .join(silver_quarantine.select("sensor_id", "timestamp_utc"), on=["sensor_id", "timestamp_utc"], how="inner")
    .count()
)

validation_results = {
    "Null sensor IDs": null_sensor_ids,
    "Duplicate sensor IDs": duplicate_sensor_ids,
    "Invalid accepted records": invalid_hourly_records,
    "Duplicate accepted sensor-hours": duplicate_hourly_records,
    "Non-unflagged accepted records": accepted_flagged_records,
    "Invalid quarantined records": invalid_quarantine_records,
    "Duplicate quarantined sensor-hours": duplicate_quarantine_records,
    "Unexpected quarantine statuses": unexpected_quarantine_statuses,
    "Overlapping accepted/quarantined records": overlapping_records,
}

for name, count in validation_results.items():
    print(f"{name}: {count:,}")

if any(validation_results.values()):
    raise ValueError(
        "Source validation failed. Investigate the reported issues before proceeding."
    )

print("All source validations passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 3: Create / refresh static sensor dimension

dim_air_quality_sensor = silver_sensor

(
    dim_air_quality_sensor.write
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

# Cell 4: Aggregate accepted and quarantined hourly measurements by sensor and date

valid_daily = (
    silver_hourly
    .groupBy("sensor_id", "local_date")
    .agg(
        F.countDistinct("timestamp_utc").alias("valid_hour_count"),
        F.countDistinct(
            F.when(F.col("has_flags") == False, F.col("timestamp_utc"))
        ).alias("unflagged_hour_count"),
        F.avg("pm25_value").alias("avg_pm25_all_valid"),
        F.min("pm25_value").alias("min_pm25_all_valid"),
        F.max("pm25_value").alias("max_pm25_all_valid"),
        F.avg(F.when(F.col("has_flags") == False, F.col("pm25_value"))).alias("avg_pm25_unflagged"),
        F.min(F.when(F.col("has_flags") == False, F.col("pm25_value"))).alias("min_pm25_unflagged"),
        F.max(F.when(F.col("has_flags") == False, F.col("pm25_value"))).alias("max_pm25_unflagged"),
        F.avg("source_hour_coverage_percent").alias("avg_source_hour_coverage_pct"),
    )
)

quarantine_daily = (
    silver_quarantine
    .groupBy("sensor_id", "local_date")
    .agg(
        F.countDistinct("timestamp_utc").alias("quarantined_hour_count"),
        F.countDistinct(
            F.when(F.col("has_flags") == True, F.col("timestamp_utc"))
        ).alias("flagged_hour_count"),
        F.countDistinct(
            F.when(F.col("quality_status") == "FLAGGED", F.col("timestamp_utc"))
        ).alias("flagged_value_hour_count"),
        F.countDistinct(
            F.when(F.col("quality_status") == "UNKNOWN_FLAG_STATUS", F.col("timestamp_utc"))
        ).alias("unknown_flag_hour_count"),
        F.countDistinct(
            F.when(F.col("quality_status") == "MISSING_VALUE", F.col("timestamp_utc"))
        ).alias("missing_value_hour_count"),
        F.countDistinct(
            F.when(
                ~F.col("quality_status").isin("FLAGGED", "UNKNOWN_FLAG_STATUS", "MISSING_VALUE"),
                F.col("timestamp_utc"),
            )
        ).alias("other_quarantined_hour_count"),
    )
)

print(f"Accepted daily sensor groups:    {valid_daily.count():,}")
print(f"Quarantined daily sensor groups: {quarantine_daily.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 5: Build complete calendar-date grid for MONTHS (preserving zero-measurement dates & DST hours)

def next_month(ms):
    y, m = int(ms[:4]), int(ms[5:7])
    return f"{y + (m == 12)}-{(m % 12) + 1:02d}-01"


ny_timezone = ZoneInfo("America/New_York")
utc_timezone = timezone.utc

expected_hours = []
month_ranges = []
for ym in sorted(MONTHS):
    month_start = f"{ym}-01"
    month_end = next_month(month_start)
    start_d = date.fromisoformat(month_start)
    end_d = date.fromisoformat(month_end)
    month_ranges.append((ym, month_start, month_end))

    current_date = start_d
    while current_date < end_d:
        start_local = datetime.combine(current_date, time.min, tzinfo=ny_timezone)
        end_local = datetime.combine(current_date + timedelta(days=1), time.min, tzinfo=ny_timezone)
        hours = int(
            (end_local.astimezone(utc_timezone) - start_local.astimezone(utc_timezone)).total_seconds() / 3600
        )
        expected_hours.append((current_date, hours))
        current_date += timedelta(days=1)

expected_hours_schema = StructType([
    StructField("local_date", DateType(), False),
    StructField("expected_hour_count", LongType(), False),
])
expected_hours_df = spark.createDataFrame(expected_hours, schema=expected_hours_schema)

sensor_ids = dim_air_quality_sensor.select("sensor_id").distinct()
daily_grid = sensor_ids.crossJoin(expected_hours_df)

fact_air_quality_daily_sensor = (
    daily_grid
    .join(valid_daily, on=["sensor_id", "local_date"], how="left")
    .join(quarantine_daily, on=["sensor_id", "local_date"], how="left")
)

count_columns = [
    "valid_hour_count",
    "unflagged_hour_count",
    "quarantined_hour_count",
    "flagged_hour_count",
    "flagged_value_hour_count",
    "unknown_flag_hour_count",
    "missing_value_hour_count",
    "other_quarantined_hour_count",
]
for column in count_columns:
    fact_air_quality_daily_sensor = fact_air_quality_daily_sensor.withColumn(
        column, F.coalesce(F.col(column), F.lit(0)).cast("long")
    )

fact_air_quality_daily_sensor = (
    fact_air_quality_daily_sensor
    .withColumn(
        "source_observed_hour_count",
        F.col("valid_hour_count") + F.col("quarantined_hour_count"),
    )
    .withColumn(
        "hour_coverage_pct",
        F.round(F.col("valid_hour_count") * 100.0 / F.col("expected_hour_count"), 2),
    )
    .withColumn(
        "unflagged_hour_coverage_pct",
        F.round(F.col("unflagged_hour_count") * 100.0 / F.col("expected_hour_count"), 2),
    )
    .withColumn(
        "source_observed_hour_coverage_pct",
        F.round(F.col("source_observed_hour_count") * 100.0 / F.col("expected_hour_count"), 2),
    )
)

SENSOR_DAILY_TABLE = "dbo.fact_air_quality_daily_sensor"
for ym, month_start, month_end in month_ranges:
    month_df = fact_air_quality_daily_sensor.filter(
        (F.col("local_date") >= month_start) & (F.col("local_date") < month_end)
    )
    if spark.catalog.tableExists(SENSOR_DAILY_TABLE):
        (
            month_df.write.format("delta").mode("overwrite")
            .option("replaceWhere", f"local_date >= '{month_start}' AND local_date < '{month_end}'")
            .option("mergeSchema", "true")
            .saveAsTable(SENSOR_DAILY_TABLE)
        )
    else:
        month_df.write.format("delta").saveAsTable(SENSOR_DAILY_TABLE)
    print(f"fact_air_quality_daily_sensor [{ym}]: saved")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 6: Validate daily sensor Gold fact against Silver for MONTHS

gold_sensor = spark.read.table("dbo.dim_air_quality_sensor")
gold_daily = (
    spark.read.table("dbo.fact_air_quality_daily_sensor")
    .filter(F.date_format("local_date", "yyyy-MM").isin(MONTHS))
)

expected_record_count = gold_sensor.select("sensor_id").distinct().count() * len(expected_hours)
actual_record_count = gold_daily.count()
if actual_record_count != expected_record_count:
    raise ValueError(
        f"Daily sensor fact record count mismatch for MONTHS: "
        f"expected {expected_record_count}, got {actual_record_count}."
    )

dup_sensor_dates = (
    gold_daily.groupBy("sensor_id", "local_date").count().filter(F.col("count") > 1).count()
)
if dup_sensor_dates > 0:
    raise ValueError(f"Found {dup_sensor_dates} duplicate sensor/date keys in daily sensor fact.")

gold_totals = gold_daily.agg(
    F.sum("valid_hour_count").alias("valid_hours"),
    F.sum("quarantined_hour_count").alias("quarantined_hours"),
    F.sum("flagged_hour_count").alias("flagged_hours"),
    F.sum("flagged_value_hour_count").alias("flagged_value_hours"),
    F.sum("unknown_flag_hour_count").alias("unknown_flag_hours"),
    F.sum("missing_value_hour_count").alias("missing_value_hours"),
).first()

expected_totals = {
    "valid_hours": silver_hourly.select("sensor_id", "timestamp_utc").distinct().count(),
    "quarantined_hours": silver_quarantine.select("sensor_id", "timestamp_utc").distinct().count(),
    "flagged_hours": silver_quarantine.filter(F.col("has_flags") == True)
    .select("sensor_id", "timestamp_utc").distinct().count(),
    "flagged_value_hours": silver_quarantine.filter(F.col("quality_status") == "FLAGGED")
    .select("sensor_id", "timestamp_utc").distinct().count(),
    "unknown_flag_hours": silver_quarantine.filter(F.col("quality_status") == "UNKNOWN_FLAG_STATUS")
    .select("sensor_id", "timestamp_utc").distinct().count(),
    "missing_value_hours": silver_quarantine.filter(F.col("quality_status") == "MISSING_VALUE")
    .select("sensor_id", "timestamp_utc").distinct().count(),
}

for metric, expected in expected_totals.items():
    actual = gold_totals[metric]
    print(f"{metric}: Gold={actual:,}, Silver={expected:,}")
    if actual != expected:
        raise ValueError(f"{metric} mismatch: Gold={actual}, Silver={expected}")

print("\nSensor-level Gold validation passed for MONTHS.")
display(
    gold_daily.select("local_date", "expected_hour_count")
    .distinct()
    .filter(F.col("expected_hour_count") != 24)
    .orderBy("local_date")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 7: Build citywide daily monitoring-network estimate for MONTHS and write month-scoped

MIN_SENSOR_COVERAGE_PCT = 75.0
MIN_ELIGIBLE_SENSORS = 4

sensor_daily = (
    spark.read.table("dbo.fact_air_quality_daily_sensor")
    .filter(F.date_format("local_date", "yyyy-MM").isin(MONTHS))
)

PM25_COLUMN = "avg_pm25_unflagged"

sensor_daily_scored = sensor_daily.withColumn(
    "is_eligible",
    (F.col("hour_coverage_pct") >= MIN_SENSOR_COVERAGE_PCT) & F.col(PM25_COLUMN).isNotNull(),
)

network_metrics = (
    sensor_daily_scored
    .groupBy("local_date")
    .agg(
        F.countDistinct(
            F.when(F.col(PM25_COLUMN).isNotNull(), F.col("sensor_id"))
        ).alias("available_sensor_count"),
        F.countDistinct(
            F.when(F.col("is_eligible"), F.col("sensor_id"))
        ).alias("eligible_sensor_count"),
        F.avg(F.when(F.col("is_eligible"), F.col(PM25_COLUMN))).alias("candidate_network_mean_pm25"),
        F.percentile_approx(
            F.when(F.col("is_eligible"), F.col(PM25_COLUMN)), 0.5, 10000
        ).alias("candidate_network_median_pm25"),
        F.min(F.when(F.col("is_eligible"), F.col(PM25_COLUMN))).alias("min_sensor_avg_pm25"),
        F.max(F.when(F.col("is_eligible"), F.col(PM25_COLUMN))).alias("max_sensor_avg_pm25"),
        F.avg(F.when(F.col("is_eligible"), F.col("hour_coverage_pct"))).alias("avg_eligible_sensor_coverage_pct"),
    )
)

date_spine = sensor_daily.select("local_date").distinct()

fact_air_quality_daily_citywide = (
    date_spine
    .join(network_metrics, on="local_date", how="left")
    .withColumn("available_sensor_count", F.coalesce(F.col("available_sensor_count"), F.lit(0)).cast("long"))
    .withColumn("eligible_sensor_count", F.coalesce(F.col("eligible_sensor_count"), F.lit(0)).cast("long"))
    .withColumn(
        "network_mean_pm25",
        F.when(F.col("eligible_sensor_count") >= MIN_ELIGIBLE_SENSORS, F.col("candidate_network_mean_pm25")),
    )
    .withColumn(
        "network_median_pm25",
        F.when(F.col("eligible_sensor_count") >= MIN_ELIGIBLE_SENSORS, F.col("candidate_network_median_pm25")),
    )
    .withColumn("minimum_sensor_coverage_pct", F.lit(MIN_SENSOR_COVERAGE_PCT))
    .withColumn("minimum_eligible_sensor_count", F.lit(MIN_ELIGIBLE_SENSORS))
    .drop("candidate_network_mean_pm25", "candidate_network_median_pm25")
)

CITYWIDE_TABLE = "dbo.fact_air_quality_daily_citywide"
for ym, month_start, month_end in month_ranges:
    month_df = fact_air_quality_daily_citywide.filter(
        (F.col("local_date") >= month_start) & (F.col("local_date") < month_end)
    )
    if spark.catalog.tableExists(CITYWIDE_TABLE):
        (
            month_df.write.format("delta").mode("overwrite")
            .option("replaceWhere", f"local_date >= '{month_start}' AND local_date < '{month_end}'")
            .option("mergeSchema", "true")
            .saveAsTable(CITYWIDE_TABLE)
        )
    else:
        month_df.write.format("delta").saveAsTable(CITYWIDE_TABLE)
    print(f"fact_air_quality_daily_citywide [{ym}]: saved")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 8: Validate citywide Gold fact for MONTHS

gold_citywide = (
    spark.read.table("dbo.fact_air_quality_daily_citywide")
    .filter(F.date_format("local_date", "yyyy-MM").isin(MONTHS))
)

duplicate_dates = (
    gold_citywide.groupBy("local_date").count().filter(F.col("count") > 1).count()
)
if duplicate_dates > 0:
    raise ValueError("Citywide fact contains duplicate dates.")

citywide_count = gold_citywide.count()
if citywide_count != len(expected_hours):
    raise ValueError(
        f"Citywide date count mismatch for MONTHS: expected {len(expected_hours)}, got {citywide_count}."
    )

invalid_estimates = gold_citywide.filter(
    (F.col("eligible_sensor_count") < MIN_ELIGIBLE_SENSORS) & F.col("network_mean_pm25").isNotNull()
).count()
if invalid_estimates > 0:
    raise ValueError(
        f"Found {invalid_estimates} days with < {MIN_ELIGIBLE_SENSORS} eligible sensors having non-null PM2.5 estimate."
    )

gold_citywide.agg(
    F.count("*").alias("total_dates"),
    F.min("local_date").alias("first_date"),
    F.max("local_date").alias("last_date"),
    F.sum(F.when(F.col("network_mean_pm25").isNotNull(), 1).otherwise(0)).alias("dates_with_network_estimate"),
    F.min("eligible_sensor_count").alias("minimum_eligible_sensors"),
    F.max("eligible_sensor_count").alias("maximum_eligible_sensors"),
).show(truncate=False)

print("ALL CHECKS PASSED")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
