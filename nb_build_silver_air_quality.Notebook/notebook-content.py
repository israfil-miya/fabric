# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "29b14a15-b502-4b45-8d4e-a0b8ee212b0f",
# META       "default_lakehouse_name": "lh_silver",
# META       "default_lakehouse_workspace_id": "14530c94-9dc4-4ef1-a320-081555064f5d",
# META       "known_lakehouses": [
# META         {
# META           "id": "29b14a15-b502-4b45-8d4e-a0b8ee212b0f"
# META         },
# META         {
# META           "id": "43446074-db34-4162-b6f2-3289971e64a2"
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

MONTHS_JSON = '["2025-11","2025-12","2026-01","2026-02","2026-03"]'
SOURCE_MODE = "auto"  # "auto" (Bronze Delta if present, else CSV), "delta", or "csv"
INSPECT_CANDIDATE_SENSORS = True


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
print("source mode:", SOURCE_MODE)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 1: Load Bronze OpenAQ data (Delta tables when available, else CSV files)

from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

spark.conf.set("spark.sql.session.timeZone", "UTC")

HOURLY_ABFS_PATH = (
    "abfss://nyc_analytics@onelake.dfs.fabric.microsoft.com/lh_bronze.Lakehouse/Files/air_quality/openaq/"
)
SOURCE_DIR = HOURLY_ABFS_PATH.rstrip("/")
LOCAL_TIMEZONE = "America/New_York"

# Selected 8 monitoring-network sensors (do not expand without formal review)
SENSOR_IDS = [
    673, 1097, 1102, 1103,
    1152, 1128, 1145, 1146,
]

BRONZE_DELTA_TABLES = {
    "hourly": "lh_bronze.dbo.bronze_openaq_hourly",
    "sensor": "lh_bronze.dbo.bronze_openaq_sensor",
    "location": "lh_bronze.dbo.bronze_openaq_location",
    "flags": "lh_bronze.dbo.bronze_openaq_flags",
}

BRONZE_CSV_FILES = {
    "hourly": "openaq_hourly_measurements.csv",
    "sensor": "openaq_sensor_metadata.csv",
    "location": "openaq_location_metadata.csv",
    "flags": "openaq_sensor_flags.csv",
}


def next_month(ms):
    y, m = int(ms[:4]), int(ms[5:7])
    return f"{y + (m == 12)}-{(m % 12) + 1:02d}-01"


def read_csv(filename):
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("multiLine", "true")
        .option("escape", '"')
        .load(f"{SOURCE_DIR}/{filename}")
    )


def month_local_bounds_utc(ym):
    """Return (start_local_date, end_local_date_exclusive, start_utc, end_utc) for YYYY-MM."""
    ny_tz = ZoneInfo(LOCAL_TIMEZONE)
    start_d = date.fromisoformat(f"{ym}-01")
    end_d = date.fromisoformat(next_month(f"{ym}-01"))
    start_local = datetime.combine(start_d, time.min, tzinfo=ny_tz)
    end_local = datetime.combine(end_d, time.min, tzinfo=ny_tz)
    return (
        start_d,
        end_d,
        start_local.astimezone(timezone.utc).replace(tzinfo=None),
        end_local.astimezone(timezone.utc).replace(tzinfo=None),
    )


delta_available = all(spark.catalog.tableExists(t) for t in BRONZE_DELTA_TABLES.values())
if SOURCE_MODE == "delta" and not delta_available:
    raise ValueError(f"SOURCE_MODE='delta' requested, but missing Bronze tables: {BRONZE_DELTA_TABLES}")
use_delta = (SOURCE_MODE == "delta") or (SOURCE_MODE == "auto" and delta_available)

if use_delta:
    hourly_raw = spark.table(BRONZE_DELTA_TABLES["hourly"])
    sensor_raw = spark.table(BRONZE_DELTA_TABLES["sensor"])
    location_raw = spark.table(BRONZE_DELTA_TABLES["location"])
    flags_raw = spark.table(BRONZE_DELTA_TABLES["flags"])
    print("Loaded Bronze OpenAQ Delta tables.")
else:
    hourly_raw = read_csv(BRONZE_CSV_FILES["hourly"])
    sensor_raw = read_csv(BRONZE_CSV_FILES["sensor"])
    location_raw = read_csv(BRONZE_CSV_FILES["location"])
    flags_raw = read_csv(BRONZE_CSV_FILES["flags"])
    print("Loaded Bronze OpenAQ CSV source files.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 2: Standardize hourly measurements and classify data quality (preserving unknown flags & multi-reasons)


def parse_iso_timestamp(column_name):
    column = F.col(column_name).cast("string")
    return F.coalesce(
        F.to_timestamp(column, "yyyy-MM-dd HH:mm:ssXXX"),
        F.to_timestamp(column, "yyyy-MM-dd'T'HH:mm:ssX"),
        F.to_timestamp(column, "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(column, "yyyy-MM-dd'T'HH:mm:ss"),
    )


def standardize_hourly(df):
    raw_flag_str = F.lower(F.trim(F.col("has_flags").cast("string")))
    flag_status_expr = (
        F.when(raw_flag_str.isin("true", "1", "yes"), F.lit("CONFIRMED_FLAGGED"))
        .when(raw_flag_str.isin("false", "0", "no"), F.lit("CONFIRMED_UNFLAGGED"))
        .otherwise(F.lit("UNKNOWN_FLAG_STATUS"))
    )
    has_flags_expr = (
        F.when(flag_status_expr == "CONFIRMED_FLAGGED", F.lit(True))
        .when(flag_status_expr == "CONFIRMED_UNFLAGGED", F.lit(False))
        .otherwise(F.lit(None).cast("boolean"))
    )
    return (
        df
        .withColumn("sensor_id", F.col("sensor_id").cast("long"))
        .withColumn("timestamp_utc", parse_iso_timestamp("datetime_from_utc"))
        .withColumn("timestamp_end_utc", parse_iso_timestamp("datetime_to_utc"))
        .withColumn("pm25_value", F.col("value").cast("double"))
        .withColumn("parameter", F.lower(F.trim(F.col("parameter").cast("string"))))
        .withColumn("unit", F.regexp_replace(F.trim(F.col("unit").cast("string")), "μ", "µ"))
        .withColumn("period_label", F.lower(F.trim(F.col("period_label").cast("string"))))
        .withColumn("raw_has_flags", F.col("has_flags").cast("string"))
        .withColumn("flag_status", flag_status_expr)
        .withColumn("has_flags", has_flags_expr)
    )


hourly_all_sensors = standardize_hourly(hourly_raw)
hourly_parsed = hourly_all_sensors.filter(F.col("sensor_id").isin(SENSOR_IDS))

invalid_timestamps = hourly_parsed.filter(
    F.col("timestamp_utc").isNull()
    | F.col("timestamp_end_utc").isNull()
    | (F.col("timestamp_end_utc") <= F.col("timestamp_utc"))
).count()

if invalid_timestamps > 0:
    raise ValueError(
        f"Found {invalid_timestamps:,} hourly records with invalid UTC intervals. "
        "Check the Bronze timestamp fields before proceeding."
    )

# Derive New York local calendar fields from UTC timestamps and filter to selected MONTHS.
hourly = (
    hourly_parsed
    .withColumn("local_timestamp", F.from_utc_timestamp(F.col("timestamp_utc"), LOCAL_TIMEZONE))
    .withColumn("local_datetime", F.date_format(F.col("local_timestamp"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("local_date", F.to_date("local_timestamp"))
    .withColumn("local_hour", F.hour("local_timestamp"))
    .withColumn("local_year_month", F.date_format(F.col("local_date"), "yyyy-MM"))
    .filter(F.col("local_year_month").isin(MONTHS))
)

# Classify all applicable quality flags without overwriting secondary issues.
QUALITY_RULE_LABELS = [
    ("is_missing_value", "MISSING_VALUE"),
    ("is_flagged", "FLAGGED"),
    ("is_flag_status_unknown", "UNKNOWN_FLAG_STATUS"),
    ("is_negative_value", "NEGATIVE_VALUE"),
    ("is_unexpected_parameter", "UNEXPECTED_PARAMETER"),
    ("is_unexpected_unit", "UNEXPECTED_UNIT"),
    ("is_unexpected_period", "UNEXPECTED_PERIOD"),
]

is_missing_val = F.col("pm25_value").isNull() | F.isnan(F.col("pm25_value"))

hourly = (
    hourly
    .withColumn("is_missing_value", is_missing_val)
    .withColumn("is_flagged", F.col("flag_status") == "CONFIRMED_FLAGGED")
    .withColumn("is_flag_status_unknown", F.col("flag_status") == "UNKNOWN_FLAG_STATUS")
    .withColumn("is_negative_value", (~is_missing_val) & (F.col("pm25_value") < 0))
    .withColumn("is_unexpected_parameter", F.coalesce(F.col("parameter"), F.lit("")) != "pm25")
    .withColumn("is_unexpected_unit", F.coalesce(F.col("unit"), F.lit("")) != "µg/m³")
    .withColumn("is_unexpected_period", F.coalesce(F.col("period_label"), F.lit("")) != "1hour")
)

reason_tokens = [F.when(F.col(col), F.lit(label)) for col, label in QUALITY_RULE_LABELS]
issue_count_expr = sum(F.col(col).cast("int") for col, _ in QUALITY_RULE_LABELS)

hourly = (
    hourly
    .withColumn("quality_issue_count", issue_count_expr)
    .withColumn("quality_reasons", F.concat_ws(",", *reason_tokens))
    .withColumn(
        "quality_status",
        F.when(F.col("quality_issue_count") == 0, F.lit("VALID"))
        .when(F.col("is_missing_value"), F.lit("MISSING_VALUE"))
        .when(F.col("is_flagged"), F.lit("FLAGGED"))
        .when(F.col("is_flag_status_unknown"), F.lit("UNKNOWN_FLAG_STATUS"))
        .when(F.col("is_negative_value"), F.lit("NEGATIVE_VALUE"))
        .when(F.col("is_unexpected_parameter"), F.lit("UNEXPECTED_PARAMETER"))
        .when(F.col("is_unexpected_unit"), F.lit("UNEXPECTED_UNIT"))
        .otherwise(F.lit("UNEXPECTED_PERIOD")),
    )
    .cache()
)

hourly_record_count = hourly.count()

# Reject duplicate sensor-hour records rather than selecting one arbitrarily.
duplicate_sensor_hours = (
    hourly
    .groupBy("sensor_id", "timestamp_utc")
    .count()
    .filter(F.col("count") > 1)
)
duplicate_count = duplicate_sensor_hours.count()
if duplicate_count > 0:
    duplicate_sensor_hours.orderBy("sensor_id", "timestamp_utc").show(20, truncate=False)
    raise ValueError(
        f"Found {duplicate_count:,} duplicate sensor-hour groups. "
        "Resolve duplicates before writing Silver."
    )

# Check UTC tail completeness dynamically for every selected month.
source_max_utc = hourly_parsed.agg(F.max("timestamp_utc").alias("max_ts")).first()["max_ts"]
for ym in MONTHS:
    _, _, _, end_utc = month_local_bounds_utc(ym)
    required_final_hour_utc = end_utc - timedelta(hours=1)
    if source_max_utc is not None and source_max_utc < required_final_hour_utc:
        print(
            f"WARNING [{ym}]: latest Bronze timestamp is {source_max_utc} UTC; "
            f"complete local month coverage requires records through {required_final_hour_utc} UTC."
        )

print(f"In-scope hourly records across selected MONTHS: {hourly_record_count:,}")
display(
    hourly
    .groupBy("local_year_month", "quality_status", "flag_status", "quality_reasons")
    .count()
    .orderBy("local_year_month", "quality_status", "flag_status")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 3: Build and validate the Silver sensor dimension (+ optional 11-sensor diagnostic comparison)

def parse_locations(df):
    return (
        df
        .withColumn("sensor_id", F.col("sensor_id").cast("long"))
        .withColumn("location_id", F.col("location_id").cast("long"))
        .withColumn("latitude", F.col("latitude").cast("double"))
        .withColumn("longitude", F.col("longitude").cast("double"))
        .withColumn(
            "is_monitor",
            F.lower(F.trim(F.coalesce(F.col("is_monitor").cast("string"), F.lit("false"))))
            .isin("true", "1", "yes"),
        )
        .withColumn(
            "is_mobile",
            F.lower(F.trim(F.coalesce(F.col("is_mobile").cast("string"), F.lit("false"))))
            .isin("true", "1", "yes"),
        )
        .withColumn("instrument_name", F.get_json_object(F.col("instruments"), "$[0].name"))
    )


all_locations = parse_locations(location_raw)
all_sensors_meta = sensor_raw.withColumn("sensor_id", F.col("sensor_id").cast("long"))

if INSPECT_CANDIDATE_SENSORS:
    print("Candidate vs selected sensor comparison across all Bronze sensors:")
    candidate_hourly_stats = (
        hourly_all_sensors
        .filter(F.col("timestamp_utc").isNotNull())
        .withColumn("local_date", F.to_date(F.from_utc_timestamp(F.col("timestamp_utc"), LOCAL_TIMEZONE)))
        .filter(F.date_format(F.col("local_date"), "yyyy-MM").isin(MONTHS))
        .groupBy("sensor_id")
        .agg(
            F.count("*").alias("in_scope_hours"),
            F.sum((F.col("flag_status") == "CONFIRMED_UNFLAGGED").cast("long")).alias("unflagged_hours"),
            F.sum((F.col("flag_status") == "CONFIRMED_FLAGGED").cast("long")).alias("flagged_hours"),
            F.sum((F.col("flag_status") == "UNKNOWN_FLAG_STATUS").cast("long")).alias("unknown_flag_hours"),
            F.sum(F.col("value").cast("double").isNull().cast("long")).alias("missing_value_hours"),
            F.round(F.avg(F.col("coverage_percent_complete").cast("double")), 2).alias("avg_within_hour_cov_pct"),
        )
    )
    candidate_comparison = (
        all_locations
        .join(
            all_sensors_meta.select("sensor_id", "first_observation_utc", "last_observation_utc"),
            on="sensor_id",
            how="outer",
        )
        .join(candidate_hourly_stats, on="sensor_id", how="left")
        .withColumn("is_selected_sensor", F.col("sensor_id").isin(SENSOR_IDS))
        .select(
            "sensor_id", "is_selected_sensor", "location_id", "location_name",
            "provider_name", "is_monitor", "is_mobile", "instrument_name",
            "latitude", "longitude", "first_observation_utc", "last_observation_utc",
            "in_scope_hours", "unflagged_hours", "flagged_hours",
            "unknown_flag_hours", "missing_value_hours", "avg_within_hour_cov_pct",
        )
        .orderBy(F.desc("is_selected_sensor"), F.col("sensor_id"))
    )
    display(candidate_comparison)

locations = all_locations.filter(F.col("sensor_id").isin(SENSOR_IDS))
sensors = (
    all_sensors_meta
    .filter(F.col("sensor_id").isin(SENSOR_IDS))
    .select("sensor_id", "first_observation_utc", "last_observation_utc")
)

expected_sensor_ids = spark.createDataFrame(
    [(sensor_id,) for sensor_id in SENSOR_IDS],
    ["sensor_id"],
)

location_duplicates = (
    locations.groupBy("sensor_id").count().filter(F.col("count") > 1).count()
)
sensor_metadata_duplicates = (
    sensors.groupBy("sensor_id").count().filter(F.col("count") > 1).count()
)
missing_location_ids = [
    row["sensor_id"]
    for row in expected_sensor_ids.join(
        locations.select("sensor_id").distinct(), on="sensor_id", how="left_anti"
    ).collect()
]

if location_duplicates > 0 or sensor_metadata_duplicates > 0:
    raise ValueError(
        "Duplicate sensor metadata found. "
        f"Location duplicates: {location_duplicates}; "
        f"sensor metadata duplicates: {sensor_metadata_duplicates}."
    )
if missing_location_ids:
    raise ValueError(f"Missing location metadata for selected sensors: {missing_location_ids}")

silver_sensor = (
    locations
    .join(sensors, on="sensor_id", how="left")
    .select(
        "sensor_id",
        "location_id",
        "location_name",
        "locality",
        "country",
        "timezone",
        "provider_name",
        "owner_name",
        "is_monitor",
        "is_mobile",
        "instrument_name",
        "latitude",
        "longitude",
        "first_observation_utc",
        "last_observation_utc",
        "instruments",
        "licenses",
    )
)

unexpected_sources = silver_sensor.filter(
    (F.coalesce(F.col("provider_name"), F.lit("")) != "AirNow")
    | (~F.coalesce(F.col("is_monitor"), F.lit(False)))
)
if unexpected_sources.count() > 0:
    raise ValueError(
        "The selected sensor metadata contains a provider other than AirNow "
        "or a non-monitor sensor. Check the selected sensor list."
    )
if silver_sensor.count() != len(SENSOR_IDS):
    raise ValueError(
        f"Expected {len(SENSOR_IDS)} sensor records, found {silver_sensor.count()}."
    )

print(f"Validated Silver sensor records: {silver_sensor.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 4: Prepare OpenAQ flag events overlapping selected MONTHS (preserving all overlapping events)

flags_parsed = (
    flags_raw
    .withColumn("queried_sensor_id", F.col("queried_sensor_id").cast("long"))
    .filter(F.col("queried_sensor_id").isin(SENSOR_IDS))
    .withColumn("flag_start_utc", parse_iso_timestamp("datetime_from_utc"))
    .withColumn("flag_end_utc", parse_iso_timestamp("datetime_to_utc"))
)

invalid_flag_starts = flags_parsed.filter(F.col("flag_start_utc").isNull()).count()
invalid_nonempty_flag_ends = flags_parsed.filter(
    F.col("datetime_to_utc").isNotNull()
    & (F.trim(F.col("datetime_to_utc").cast("string")) != "")
    & F.col("flag_end_utc").isNull()
).count()

if invalid_flag_starts > 0 or invalid_nonempty_flag_ends > 0:
    raise ValueError(
        "Some OpenAQ flag timestamps could not be parsed. "
        f"Invalid starts: {invalid_flag_starts}; "
        f"invalid non-empty ends: {invalid_nonempty_flag_ends}."
    )

flags_with_local_times = (
    flags_parsed
    .withColumn("flag_start_local", F.from_utc_timestamp(F.col("flag_start_utc"), LOCAL_TIMEZONE))
    .withColumn(
        "flag_end_local",
        F.when(
            F.col("flag_end_utc").isNotNull(),
            F.from_utc_timestamp(F.col("flag_end_utc"), LOCAL_TIMEZONE),
        ),
    )
)

# Overlap condition across any selected month (including events that started earlier).
month_overlap_cond = None
for ym in MONTHS:
    start_d, end_d, _, _ = month_local_bounds_utc(ym)
    m_start_ts = F.to_timestamp(F.lit(f"{start_d.isoformat()} 00:00:00"))
    m_end_ts = F.to_timestamp(F.lit(f"{end_d.isoformat()} 00:00:00"))
    cond = (
        (F.col("flag_start_local") < m_end_ts)
        & (F.col("flag_end_local").isNull() | (F.col("flag_end_local") >= m_start_ts))
    )
    month_overlap_cond = cond if month_overlap_cond is None else (month_overlap_cond | cond)

flags = (
    flags_with_local_times
    .filter(month_overlap_cond)
    .select(
        "queried_sensor_id",
        F.col("location_id").cast("long").alias("location_id"),
        "flag_type_id",
        "flag_label",
        "flag_level",
        "flag_start_utc",
        "flag_end_utc",
        "datetime_from_local",
        "datetime_to_local",
        "sensor_ids",
        "note",
    )
)

print(f"Prepared OpenAQ flag events overlapping MONTHS: {flags.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 5: Enrich hourly observations with sensor metadata and overlapping flag-event summaries

# Attach active interval flag labels per (sensor_id, timestamp_utc) without collapsing multiple flags.
hourly_flag_events = (
    hourly.select("sensor_id", "timestamp_utc", "timestamp_end_utc").alias("obs")
    .join(
        flags.alias("f"),
        (F.col("obs.sensor_id") == F.col("f.queried_sensor_id"))
        & (F.col("f.flag_start_utc") < F.col("obs.timestamp_end_utc"))
        & (F.col("f.flag_end_utc").isNull() | (F.col("f.flag_end_utc") > F.col("obs.timestamp_utc"))),
        how="inner",
    )
    .groupBy("obs.sensor_id", "obs.timestamp_utc")
    .agg(
        F.count("*").cast("long").alias("active_flag_event_count"),
        F.concat_ws(",", F.sort_array(F.collect_set("f.flag_label"))).alias("active_flag_labels"),
    )
)

hourly_enriched = (
    hourly.alias("h")
    .join(hourly_flag_events.alias("fe"), on=["sensor_id", "timestamp_utc"], how="left")
    .withColumn("active_flag_event_count", F.coalesce(F.col("fe.active_flag_event_count"), F.lit(0)).cast("long"))
    .withColumn("active_flag_labels", F.coalesce(F.col("fe.active_flag_labels"), F.lit("")))
)

valid_hourly = hourly_enriched.filter(F.col("quality_status") == "VALID")
quarantined_hourly = hourly_enriched.filter(F.col("quality_status") != "VALID")

unmatched_hourly_sensors = (
    valid_hourly
    .select("sensor_id")
    .distinct()
    .join(silver_sensor.select("sensor_id"), on="sensor_id", how="left_anti")
    .count()
)
if unmatched_hourly_sensors > 0:
    raise ValueError(
        f"Found {unmatched_hourly_sensors} sensor IDs in hourly data without matching sensor metadata."
    )

sensor_lookup = silver_sensor.select(
    "sensor_id",
    "location_id",
    "location_name",
    "provider_name",
    "owner_name",
    "instrument_name",
    "is_monitor",
    "latitude",
    "longitude",
)

h = valid_hourly.alias("h")
s = sensor_lookup.alias("s")

silver_hourly = (
    h.join(s, F.col("h.sensor_id") == F.col("s.sensor_id"), "inner")
    .select(
        F.col("h.sensor_id"),
        F.col("s.location_id"),
        F.coalesce(F.col("s.location_name"), F.col("h.location_name")).alias("location_name"),
        F.col("s.provider_name"),
        F.col("s.owner_name"),
        F.col("s.instrument_name"),
        F.col("s.is_monitor"),
        F.col("s.latitude"),
        F.col("s.longitude"),
        F.col("h.timestamp_utc"),
        F.col("h.timestamp_end_utc"),
        F.col("h.local_datetime"),
        F.col("h.local_date"),
        F.col("h.local_hour"),
        F.col("h.local_year_month"),
        F.col("h.pm25_value"),
        F.col("h.unit"),
        F.col("h.parameter"),
        F.col("h.has_flags"),
        F.col("h.flag_status"),
        F.col("h.raw_has_flags"),
        F.col("h.active_flag_event_count"),
        F.col("h.active_flag_labels"),
        F.col("h.summary_min").cast("double").alias("within_hour_min"),
        F.col("h.summary_max").cast("double").alias("within_hour_max"),
        F.col("h.summary_mean").cast("double").alias("within_hour_mean"),
        F.col("h.coverage_percent_complete").cast("double").alias("source_hour_coverage_percent"),
    )
)

q = quarantined_hourly.alias("q")
qs = sensor_lookup.select("sensor_id", "location_id", "location_name").alias("qs")

quarantined_hourly = (
    q.join(qs, F.col("q.sensor_id") == F.col("qs.sensor_id"), "left")
    .select(
        F.col("q.sensor_id"),
        F.col("qs.location_id").cast("long").alias("location_id"),
        F.coalesce(F.col("qs.location_name"), F.col("q.location_name")).alias("location_name"),
        F.col("q.timestamp_utc"),
        F.col("q.timestamp_end_utc"),
        F.col("q.local_datetime"),
        F.col("q.local_date"),
        F.col("q.local_hour"),
        F.col("q.local_year_month"),
        F.col("q.pm25_value"),
        F.col("q.unit"),
        F.col("q.parameter"),
        F.col("q.period_label"),
        F.col("q.has_flags"),
        F.col("q.flag_status"),
        F.col("q.raw_has_flags"),
        F.col("q.quality_status"),
        F.col("q.quality_reasons"),
        F.col("q.quality_issue_count"),
        F.col("q.is_missing_value"),
        F.col("q.is_flagged"),
        F.col("q.is_flag_status_unknown"),
        F.col("q.is_negative_value"),
        F.col("q.is_unexpected_parameter"),
        F.col("q.is_unexpected_unit"),
        F.col("q.is_unexpected_period"),
        F.col("q.active_flag_event_count"),
        F.col("q.active_flag_labels"),
        F.col("q.summary_min").cast("double").alias("within_hour_min"),
        F.col("q.summary_max").cast("double").alias("within_hour_max"),
        F.col("q.summary_mean").cast("double").alias("within_hour_mean"),
        F.col("q.coverage_percent_complete").cast("double").alias("source_hour_coverage_percent"),
    )
)

# Validate Silver datasets prior to writing.
overlapping_hourly_records = (
    silver_hourly.select("sensor_id", "timestamp_utc")
    .join(quarantined_hourly.select("sensor_id", "timestamp_utc"), on=["sensor_id", "timestamp_utc"], how="inner")
    .count()
)
accepted_non_unflagged = silver_hourly.filter(
    (F.col("flag_status") != "CONFIRMED_UNFLAGGED") | (F.col("has_flags") != False) | F.col("has_flags").isNull()
).count()
invalid_accepted_records = silver_hourly.filter(
    F.col("pm25_value").isNull()
    | (F.col("pm25_value") < 0)
    | (F.col("parameter") != "pm25")
    | (F.col("unit") != "µg/m³")
).count()
invalid_quarantined_records = quarantined_hourly.filter(F.col("quality_status") == "VALID").count()

if any([overlapping_hourly_records, accepted_non_unflagged, invalid_accepted_records, invalid_quarantined_records]):
    raise ValueError(
        "Silver output validation failed. "
        f"Overlapping={overlapping_hourly_records}, accepted_non_unflagged={accepted_non_unflagged}, "
        f"invalid_accepted={invalid_accepted_records}, valid_in_quarantine={invalid_quarantined_records}."
    )

print(f"Prepared accepted Silver hourly records: {silver_hourly.count():,}")
print(f"Prepared quarantined hourly records:    {quarantined_hourly.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 6: Month-scoped writes and reconciliation audit

SILVER_HOURLY_TABLE = "dbo.silver_air_quality_hourly"
SILVER_QUARANTINE_TABLE = "dbo.silver_air_quality_quarantine"
SILVER_SENSOR_TABLE = "dbo.silver_air_quality_sensor"
SILVER_FLAGS_TABLE = "dbo.silver_air_quality_flags"
SILVER_AUDIT_TABLE = "dbo.silver_air_quality_quality_audit"


def write_date_range_month(df, table_name, month_start, month_end):
    """Replace only [month_start, month_end) rows by local_date, preserving other months."""
    if spark.catalog.tableExists(table_name):
        (
            df.write.format("delta").mode("overwrite")
            .option("replaceWhere", f"local_date >= '{month_start}' AND local_date < '{month_end}'")
            .option("mergeSchema", "true")
            .saveAsTable(table_name)
        )
    else:
        df.write.format("delta").saveAsTable(table_name)


# 1. Sensor dimension (static validated dimension)
(
    silver_sensor.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_SENSOR_TABLE)
)

# 2. Flags table (preserve existing flags outside selected MONTHS, update overlapping flags cleanly)
if spark.catalog.tableExists(SILVER_FLAGS_TABLE):
    existing_flags = spark.table(SILVER_FLAGS_TABLE)
    existing_local = (
        existing_flags
        .withColumn("flag_start_local", F.from_utc_timestamp(F.col("flag_start_utc"), LOCAL_TIMEZONE))
        .withColumn(
            "flag_end_local",
            F.when(
                F.col("flag_end_utc").isNotNull(),
                F.from_utc_timestamp(F.col("flag_end_utc"), LOCAL_TIMEZONE),
            ),
        )
    )
    non_overlapping_existing = existing_local.filter(~month_overlap_cond).select(*flags.columns)
    merged_flags = non_overlapping_existing.unionByName(flags).dropDuplicates([
        "queried_sensor_id", "location_id", "flag_type_id", "flag_label",
        "flag_level", "flag_start_utc", "flag_end_utc",
    ])
else:
    merged_flags = flags

(
    merged_flags.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_FLAGS_TABLE)
)

AUDIT_SCHEMA = StructType([
    StructField("local_year_month", StringType(), False),
    StructField("audit_timestamp_utc", TimestampType(), False),
    StructField("in_scope_bronze_count", LongType(), False),
    StructField("accepted_count", LongType(), False),
    StructField("quarantined_count", LongType(), False),
    StructField("written_accepted_count", LongType(), False),
    StructField("written_quarantine_count", LongType(), False),
    StructField("flagged_count", LongType(), False),
    StructField("unknown_flag_status_count", LongType(), False),
    StructField("missing_value_count", LongType(), False),
    StructField("negative_value_count", LongType(), False),
    StructField("unexpected_parameter_count", LongType(), False),
    StructField("unexpected_unit_count", LongType(), False),
    StructField("unexpected_period_count", LongType(), False),
    StructField("multi_issue_count", LongType(), False),
    StructField("avg_within_hour_coverage_pct", DoubleType(), True),
    StructField("missing_within_hour_cov_count", LongType(), False),
    StructField("reconciliation_diff", LongType(), False),
    StructField("reconciliation_passed", BooleanType(), False),
])

# 3. Month-scoped writes and reconciliation for hourly, quarantine, and quality audit
audit_rows = []
for ym in MONTHS:
    start_d, end_d, _, _ = month_local_bounds_utc(ym)
    month_start = start_d.isoformat()
    month_end = end_d.isoformat()

    m_bronze_n = hourly.filter(F.col("local_year_month") == ym).count()
    m_accepted = silver_hourly.filter(F.col("local_year_month") == ym)
    m_quarantine = quarantined_hourly.filter(F.col("local_year_month") == ym)

    m_accepted_n = m_accepted.count()
    m_quarantine_n = m_quarantine.count()

    write_date_range_month(m_accepted, SILVER_HOURLY_TABLE, month_start, month_end)
    write_date_range_month(m_quarantine, SILVER_QUARANTINE_TABLE, month_start, month_end)

    written_accepted_n = (
        spark.table(SILVER_HOURLY_TABLE)
        .filter((F.col("local_date") >= month_start) & (F.col("local_date") < month_end))
        .count()
    )
    written_quarantine_n = (
        spark.table(SILVER_QUARANTINE_TABLE)
        .filter((F.col("local_date") >= month_start) & (F.col("local_date") < month_end))
        .count()
    )

    reconciliation_diff = m_bronze_n - (written_accepted_n + written_quarantine_n)
    reconciliation_passed = (
        written_accepted_n == m_accepted_n
        and written_quarantine_n == m_quarantine_n
        and reconciliation_diff == 0
    )

    q_stats = (
        hourly.filter(F.col("local_year_month") == ym)
        .agg(
            F.sum(F.col("is_flagged").cast("long")).alias("flagged_count"),
            F.sum(F.col("is_flag_status_unknown").cast("long")).alias("unknown_flag_status_count"),
            F.sum(F.col("is_missing_value").cast("long")).alias("missing_value_count"),
            F.sum(F.col("is_negative_value").cast("long")).alias("negative_value_count"),
            F.sum(F.col("is_unexpected_parameter").cast("long")).alias("unexpected_parameter_count"),
            F.sum(F.col("is_unexpected_unit").cast("long")).alias("unexpected_unit_count"),
            F.sum(F.col("is_unexpected_period").cast("long")).alias("unexpected_period_count"),
            F.sum((F.col("quality_issue_count") > 1).cast("long")).alias("multi_issue_count"),
            F.round(F.avg(F.col("coverage_percent_complete").cast("double")), 2).alias("avg_within_hour_coverage_pct"),
            F.sum(F.col("coverage_percent_complete").cast("double").isNull().cast("long")).alias("missing_within_hour_cov_count"),
        )
        .first()
        .asDict()
    )

    audit_row = {
        "local_year_month": ym,
        "audit_timestamp_utc": datetime.now(timezone.utc).replace(tzinfo=None),
        "in_scope_bronze_count": int(m_bronze_n),
        "accepted_count": int(m_accepted_n),
        "quarantined_count": int(m_quarantine_n),
        "written_accepted_count": int(written_accepted_n),
        "written_quarantine_count": int(written_quarantine_n),
        "flagged_count": int(q_stats["flagged_count"] or 0),
        "unknown_flag_status_count": int(q_stats["unknown_flag_status_count"] or 0),
        "missing_value_count": int(q_stats["missing_value_count"] or 0),
        "negative_value_count": int(q_stats["negative_value_count"] or 0),
        "unexpected_parameter_count": int(q_stats["unexpected_parameter_count"] or 0),
        "unexpected_unit_count": int(q_stats["unexpected_unit_count"] or 0),
        "unexpected_period_count": int(q_stats["unexpected_period_count"] or 0),
        "multi_issue_count": int(q_stats["multi_issue_count"] or 0),
        "avg_within_hour_coverage_pct": float(q_stats["avg_within_hour_coverage_pct"])
        if q_stats["avg_within_hour_coverage_pct"] is not None else None,
        "missing_within_hour_cov_count": int(q_stats["missing_within_hour_cov_count"] or 0),
        "reconciliation_diff": int(reconciliation_diff),
        "reconciliation_passed": bool(reconciliation_passed),
    }
    audit_rows.append(audit_row)

    audit_month_df = spark.createDataFrame([audit_row], schema=AUDIT_SCHEMA)
    if spark.catalog.tableExists(SILVER_AUDIT_TABLE):
        (
            audit_month_df.write.format("delta").mode("overwrite")
            .option("replaceWhere", f"local_year_month = '{ym}'")
            .option("mergeSchema", "true")
            .saveAsTable(SILVER_AUDIT_TABLE)
        )
    else:
        audit_month_df.write.format("delta").saveAsTable(SILVER_AUDIT_TABLE)

    if not reconciliation_passed:
        raise ValueError(
            f"Air quality Silver reconciliation failed for {ym}: "
            f"bronze={m_bronze_n}, accepted={written_accepted_n}/{m_accepted_n}, "
            f"quarantine={written_quarantine_n}/{m_quarantine_n}"
        )
    print(
        f"{ym}: bronze={m_bronze_n:,}  accepted={written_accepted_n:,}  "
        f"quarantined={written_quarantine_n:,}  diff={reconciliation_diff}"
    )

hourly.unpersist()

print("\nSilver Air Quality Audit Summary:")
(
    spark.table(SILVER_AUDIT_TABLE)
    .filter(F.col("local_year_month").isin(MONTHS))
    .orderBy("local_year_month")
    .show(truncate=False)
)

# Quarantine breakdown for auditability across selected MONTHS.
display(
    spark.table(SILVER_QUARANTINE_TABLE)
    .filter(F.date_format("local_date", "yyyy-MM").isin(MONTHS))
    .groupBy("quality_status", "flag_status", "quality_reasons")
    .count()
    .orderBy("quality_status", "flag_status")
)

print("All saved OpenAQ Silver tables passed month-scoped reconciliation.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
