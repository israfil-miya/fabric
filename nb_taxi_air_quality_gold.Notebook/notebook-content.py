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

# MARKDOWN ********************

# ## Configuration
# The pipeline passes the resolved month list as JSON. Set `MONTHS_JSON` when running interactively.

# PARAMETERS CELL ********************

MONTHS_JSON = '["2025-11","2025-12","2026-01","2026-02","2026-03"]'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import re


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

# Cell 1: Build combined taxi and air-quality daily fact with month-scoped replacement

from datetime import date
from pyspark.sql import functions as F

TARGET_TABLE = "dbo.fact_taxi_air_quality_daily"


def next_month(ms):
    y, m = int(ms[:4]), int(ms[5:7])
    return f"{y + (m == 12)}-{(m % 12) + 1:02d}-01"


taxi_daily_all = spark.table("dbo.fact_taxi_daily")
air_quality_all = spark.table("dbo.fact_air_quality_daily_citywide")
dim_date_all = spark.table("dbo.dim_date")

month_expectations = {}
for ym in sorted(MONTHS):
    month_start = f"{ym}-01"
    month_end = next_month(month_start)
    expected_days = (date.fromisoformat(month_end) - date.fromisoformat(month_start)).days
    lo = int(month_start.replace("-", ""))
    hi = int(month_end.replace("-", ""))
    month_expectations[ym] = (month_start, month_end, lo, hi, expected_days)

    dim_month = dim_date_all.filter(F.col("year_month") == ym).select("date_key", "full_date")
    if dim_month.count() != expected_days:
        raise ValueError(f"dim_date incomplete for {ym}: expected {expected_days} days")

    taxi_month = taxi_daily_all.filter((F.col("date_key") >= lo) & (F.col("date_key") < hi))
    if taxi_month.count() != expected_days:
        raise ValueError(
            f"fact_taxi_daily incomplete for {ym}: expected {expected_days} days, got {taxi_month.count()}"
        )

    aq_month = air_quality_all.filter(
        (F.col("local_date") >= month_start) & (F.col("local_date") < month_end)
    )
    if aq_month.count() != expected_days:
        raise ValueError(
            f"fact_air_quality_daily_citywide incomplete for {ym}: "
            f"expected {expected_days} days, got {aq_month.count()}"
        )

    aq_with_key = (
        aq_month.alias("aq")
        .join(dim_month.alias("d"), F.col("aq.local_date") == F.col("d.full_date"), "inner")
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
            "aq.minimum_eligible_sensor_count",
        )
    )

    # Left join preserves every taxi date; unavailable air-quality estimates stay NULL (never 0).
    combined_month = taxi_month.join(aq_with_key, on="date_key", how="left")

    if spark.catalog.tableExists(TARGET_TABLE):
        (
            combined_month.write.format("delta").mode("overwrite")
            .option("replaceWhere", f"date_key >= {lo} AND date_key < {hi}")
            .option("mergeSchema", "true")
            .saveAsTable(TARGET_TABLE)
        )
    else:
        combined_month.write.format("delta").saveAsTable(TARGET_TABLE)

    print(f"{ym}: wrote {expected_days} combined daily rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 2: Validate the combined Gold fact scoped to MONTHS

taxi = spark.table("dbo.fact_taxi_daily")
combined = spark.table(TARGET_TABLE)
dim_date = spark.table("dbo.dim_date").filter(F.col("year_month").isin(MONTHS))

taxi_scoped = taxi.join(dim_date.select("date_key", "year_month"), "date_key")
combined_scoped = combined.join(dim_date.select("date_key", "year_month"), "date_key")

taxi_count = taxi_scoped.count()
combined_count = combined_scoped.count()
expected_total_days = sum(v[4] for v in month_expectations.values())

duplicate_dates = (
    combined_scoped.groupBy("date_key").count().filter(F.col("count") > 1).count()
)
missing_aq_join = combined_scoped.filter(F.col("eligible_sensor_count").isNull()).count()
bad_null_pm25 = combined_scoped.filter(
    (F.col("eligible_sensor_count") < F.col("minimum_eligible_sensor_count"))
    & F.col("network_mean_pm25").isNotNull()
).count()

print("Expected calendar days in MONTHS:", expected_total_days)
print("Taxi fact rows in MONTHS:        ", taxi_count)
print("Combined fact rows in MONTHS:    ", combined_count)
print("Duplicate date keys in MONTHS:   ", duplicate_dates)
print("Rows missing AQ join metadata:   ", missing_aq_join)

(
    combined_scoped.groupBy("year_month")
    .agg(
        F.count("*").alias("total_dates"),
        F.count("network_mean_pm25").alias("dates_with_pm25"),
        F.sum(F.when(F.col("network_mean_pm25").isNull(), 1).otherwise(0)).alias("dates_without_pm25"),
    )
    .orderBy("year_month")
    .show(truncate=False)
)

assert taxi_count == expected_total_days, f"Taxi count {taxi_count} != {expected_total_days}"
assert combined_count == taxi_count, f"Combined count {combined_count} != {taxi_count}"
assert duplicate_dates == 0, f"Duplicate date keys found: {duplicate_dates}"
assert missing_aq_join == 0, f"Missing AQ metadata on {missing_aq_join} dates"
assert bad_null_pm25 == 0, f"Found {bad_null_pm25} non-null PM2.5 estimates below minimum sensors"

print("ALL CHECKS PASSED")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
