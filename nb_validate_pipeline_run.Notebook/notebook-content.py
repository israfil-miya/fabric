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
# META           "id": "43446074-db34-4162-b6f2-3289971e64a2"
# META         },
# META         {
# META           "id": "29b14a15-b502-4b45-8d4e-a0b8ee212b0f"
# META         },
# META         {
# META           "id": "9d65ce6c-ba84-4650-9668-115a27ff14a7"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Configuration
# Validates end-to-end source-to-target reconciliation for `MONTHS` and logs run results to `dbo.etl_run_log`.

# PARAMETERS CELL ********************

import json
import re

MONTHS_JSON = "[]"
RUN_ID = ""
PIPELINE_START_UTC = ""


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
print("months to validate:", MONTHS)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 1: Cross-layer reconciliation across Taxi, OpenAQ, Economics, and Combined Gold

import uuid
from datetime import date, datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

run_id = RUN_ID.strip() if RUN_ID and RUN_ID.strip() else str(uuid.uuid4())
end_time_utc = datetime.now(timezone.utc).replace(tzinfo=None)
try:
    start_time_utc = (
        datetime.fromisoformat(PIPELINE_START_UTC.strip().replace("Z", "+00:00"))
        .astimezone(timezone.utc)
        .replace(tzinfo=None)
        if PIPELINE_START_UTC and PIPELINE_START_UTC.strip()
        else end_time_utc
    )
except ValueError:
    start_time_utc = end_time_utc


def next_month(ms):
    y, m = int(ms[:4]), int(ms[5:7])
    return f"{y + (m == 12)}-{(m % 12) + 1:02d}-01"


problems = []
log_entries = []

dim_date = spark.table("dbo.dim_date").filter(F.col("year_month").isin(MONTHS))
expected_days_by_month = {
    ym: (date.fromisoformat(next_month(f"{ym}-01")) - date.fromisoformat(f"{ym}-01")).days
    for ym in MONTHS
}

# ---- 1. Taxi Bronze -> Silver -> Gold reconciliation ----
taxi_audit = {
    r["source_year_month"]: r
    for r in spark.table("lh_silver.dbo.silver_taxi_quality_audit")
    .filter(F.col("source_year_month").isin(MONTHS))
    .collect()
}
taxi_daily_m = {
    r["year_month"]: r
    for r in spark.table("dbo.fact_taxi_daily")
    .join(dim_date.select("date_key", "year_month"), "date_key")
    .groupBy("year_month")
    .agg(F.count("*").alias("days"), F.sum("trip_count").alias("trips"))
    .collect()
}
taxi_hz_m = {
    r["year_month"]: r
    for r in spark.table("dbo.fact_taxi_hourly_zone")
    .join(dim_date.select("date_key", "year_month"), "date_key")
    .groupBy("year_month")
    .agg(F.sum("trip_count").alias("trips"))
    .collect()
}

for ym in MONTHS:
    err = []
    a = taxi_audit.get(ym)
    d = taxi_daily_m.get(ym)
    hz = taxi_hz_m.get(ym)
    if a is None or not a["reconciliation_passed"] or a["bronze_row_count"] <= 0:
        err.append("Silver taxi audit missing or failed")
    elif d is None or d["days"] != expected_days_by_month[ym] or d["trips"] != a["written_silver_count"]:
        err.append(f"Gold daily mismatch (daily={d}, silver={a['written_silver_count']})")
    elif hz is None or hz["trips"] != d["trips"]:
        err.append(f"Gold hourly_zone mismatch (hz={hz}, daily={d['trips']})")
    if err:
        problems.append(f"NYC_TAXI [{ym}]: " + "; ".join(err))
    log_entries.append({
        "run_id": run_id,
        "source_name": "NYC_TAXI",
        "selected_month": ym,
        "start_time_utc": start_time_utc,
        "end_time_utc": end_time_utc,
        "status": "FAILED" if err else "SUCCEEDED",
        "records_read": int(a["bronze_row_count"]) if a else 0,
        "records_written": int(d["trips"]) if d and d["trips"] is not None else 0,
        "error_message": "; ".join(err) if err else None,
    })

# ---- 2. OpenAQ Bronze -> Silver -> Gold reconciliation ----
aq_bronze_audit = {}
if spark.catalog.tableExists("lh_bronze.dbo.bronze_openaq_ingest_audit"):
    for r in (
        spark.table("lh_bronze.dbo.bronze_openaq_ingest_audit")
        .filter(F.col("year_month").isin(MONTHS))
        .groupBy("year_month")
        .agg(
            F.count("*").alias("sensor_rows"),
            F.sum((F.col("status") != "SUCCESS").cast("long")).alias("failed_sensors"),
            F.sum("api_total_records").alias("api_total"),
            F.sum("in_month_records").alias("in_month"),
            F.sum("out_of_window_records").alias("out_window"),
        )
        .collect()
    ):
        aq_bronze_audit[r["year_month"]] = r

aq_silver_audit = {
    r["local_year_month"]: r
    for r in spark.table("lh_silver.dbo.silver_air_quality_quality_audit")
    .filter(F.col("local_year_month").isin(MONTHS))
    .collect()
}
aq_sensor_count = spark.table("dbo.dim_air_quality_sensor").select("sensor_id").distinct().count()
aq_daily_sensor_m = {
    r["ym"]: r
    for r in spark.table("dbo.fact_air_quality_daily_sensor")
    .withColumn("ym", F.date_format("local_date", "yyyy-MM"))
    .filter(F.col("ym").isin(MONTHS))
    .groupBy("ym")
    .agg(
        F.count("*").alias("rows"),
        F.sum("valid_hour_count").alias("valid_hours"),
        F.sum("quarantined_hour_count").alias("quarantined_hours"),
    )
    .collect()
}
aq_citywide_m = {
    r["ym"]: r
    for r in spark.table("dbo.fact_air_quality_daily_citywide")
    .withColumn("ym", F.date_format("local_date", "yyyy-MM"))
    .filter(F.col("ym").isin(MONTHS))
    .groupBy("ym")
    .agg(F.count("*").alias("days"))
    .collect()
}

for ym in MONTHS:
    err = []
    ba = aq_bronze_audit.get(ym)
    sa = aq_silver_audit.get(ym)
    ds = aq_daily_sensor_m.get(ym)
    cw = aq_citywide_m.get(ym)
    if ba is not None:
        if ba["sensor_rows"] < aq_sensor_count or ba["failed_sensors"] > 0:
            err.append(f"OpenAQ Bronze sensor audit incomplete/failed: {ba}")
        elif ba["api_total"] != (ba["in_month"] + ba["out_window"]):
            err.append("OpenAQ Bronze API total != in_month + out_of_window")
    if sa is None or not sa["reconciliation_passed"]:
        err.append("Silver air quality audit missing or failed")
    elif ds is None or ds["rows"] != (aq_sensor_count * expected_days_by_month[ym]):
        err.append(f"Gold daily sensor row count mismatch for {ym}")
    elif ds["valid_hours"] != sa["written_accepted_count"] or ds["quarantined_hours"] != sa["written_quarantine_count"]:
        err.append("Gold air quality hours do not match Silver accepted/quarantined counts")
    elif cw is None or cw["days"] != expected_days_by_month[ym]:
        err.append(f"Gold citywide date count mismatch for {ym}")
    if err:
        problems.append(f"OPENAQ [{ym}]: " + "; ".join(err))
    log_entries.append({
        "run_id": run_id,
        "source_name": "OPENAQ",
        "selected_month": ym,
        "start_time_utc": start_time_utc,
        "end_time_utc": end_time_utc,
        "status": "FAILED" if err else "SUCCEEDED",
        "records_read": int(sa["in_scope_bronze_count"]) if sa else 0,
        "records_written": int((ds["valid_hours"] or 0) + (ds["quarantined_hours"] or 0)) if ds else 0,
        "error_message": "; ".join(err) if err else None,
    })

# ---- 3. Combined Taxi-Air Quality Gold ----
combined_m = {
    r["year_month"]: r
    for r in spark.table("dbo.fact_taxi_air_quality_daily")
    .join(dim_date.select("date_key", "year_month"), "date_key")
    .groupBy("year_month")
    .agg(F.count("*").alias("days"), F.countDistinct("date_key").alias("distinct_days"))
    .collect()
}
for ym in MONTHS:
    err = []
    cm = combined_m.get(ym)
    if cm is None or cm["days"] != expected_days_by_month[ym] or cm["days"] != cm["distinct_days"]:
        err.append(f"Combined fact mismatch for {ym}: {cm}")
    if err:
        problems.append(f"COMBINED_TAXI_AQ [{ym}]: " + "; ".join(err))
    log_entries.append({
        "run_id": run_id,
        "source_name": "COMBINED_TAXI_AQ",
        "selected_month": ym,
        "start_time_utc": start_time_utc,
        "end_time_utc": end_time_utc,
        "status": "FAILED" if err else "SUCCEEDED",
        "records_read": int(taxi_daily_m[ym]["days"]) if ym in taxi_daily_m else 0,
        "records_written": int(cm["days"]) if cm else 0,
        "error_message": "; ".join(err) if err else None,
    })

# ---- 4. World Bank GDP & ECB FX reconciliation ----
econ_audit = {
    r["dataset"]: r
    for r in spark.table("lh_silver.dbo.silver_economics_quality_audit").collect()
}
gdp_gold_n = spark.table("dbo.fact_gdp_annual").count()
gdp_audit = econ_audit.get("WORLD_BANK_GDP")
gdp_err = []
if gdp_audit is None or not gdp_audit["reconciliation_passed"]:
    gdp_err.append("Silver GDP audit missing or failed")
elif gdp_gold_n != gdp_audit["silver_row_count"]:
    gdp_err.append(f"fact_gdp_annual count {gdp_gold_n} != Silver {gdp_audit['silver_row_count']}")
if gdp_err:
    problems.append("WORLD_BANK_GDP: " + "; ".join(gdp_err))
log_entries.append({
    "run_id": run_id,
    "source_name": "WORLD_BANK_GDP",
    "selected_month": ",".join(MONTHS),
    "start_time_utc": start_time_utc,
    "end_time_utc": end_time_utc,
    "status": "FAILED" if gdp_err else "SUCCEEDED",
    "records_read": int(gdp_audit["bronze_row_count"]) if gdp_audit else 0,
    "records_written": int(gdp_gold_n),
    "error_message": "; ".join(gdp_err) if gdp_err else None,
})

fx_audit = econ_audit.get("ECB_FX")
fx_gold_m = {
    r["year_month"]: r
    for r in spark.table("dbo.fact_fx_daily")
    .join(dim_date.select("date_key", "year_month"), "date_key")
    .groupBy("year_month")
    .agg(
        F.count("*").alias("rows"),
        F.sum(F.col("usd_per_eur_rate").isNull().cast("long")).alias("null_rates"),
    )
    .collect()
}
for ym in MONTHS:
    err = []
    fxm = fx_gold_m.get(ym)
    if fx_audit is None or not fx_audit["reconciliation_passed"]:
        err.append("Silver ECB FX audit missing or failed")
    elif fxm is None or fxm["rows"] != expected_days_by_month[ym] or fxm["null_rates"] > 0:
        err.append(f"fact_fx_daily incomplete for {ym}: {fxm}")
    if err:
        problems.append(f"ECB_FX [{ym}]: " + "; ".join(err))
    log_entries.append({
        "run_id": run_id,
        "source_name": "ECB_FX",
        "selected_month": ym,
        "start_time_utc": start_time_utc,
        "end_time_utc": end_time_utc,
        "status": "FAILED" if err else "SUCCEEDED",
        "records_read": int(fx_audit["bronze_row_count"]) if fx_audit else 0,
        "records_written": int(fxm["rows"]) if fxm else 0,
        "error_message": "; ".join(err) if err else None,
    })

LOG_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("source_name", StringType(), False),
    StructField("selected_month", StringType(), False),
    StructField("start_time_utc", TimestampType(), False),
    StructField("end_time_utc", TimestampType(), False),
    StructField("status", StringType(), False),
    StructField("records_read", LongType(), False),
    StructField("records_written", LongType(), False),
    StructField("error_message", StringType(), True),
])

log_df = spark.createDataFrame(log_entries, schema=LOG_SCHEMA)
(
    log_df.write.format("delta").mode("append")
    .option("mergeSchema", "true")
    .saveAsTable("dbo.etl_run_log")
)

log_df.orderBy("source_name", "selected_month").show(100, truncate=False)

if problems:
    raise ValueError("PIPELINE VALIDATION FAILED: " + " | ".join(problems))

print("ALL CHECKS PASSED")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
