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

# CELL ********************

# Cell 1: Configuration and load Bronze source files

from datetime import datetime
from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")

HOURLY_ABFS_PATH = (
    "abfss://nyc_analytics@onelake.dfs.fabric.microsoft.com/lh_bronze.Lakehouse/Files/air_quality/openaq/"
)

SOURCE_DIR = HOURLY_ABFS_PATH.rstrip("/")

START_LOCAL_DATE = "2025-11-01"
END_LOCAL_DATE_EXCLUSIVE = "2026-04-01"
LOCAL_TIMEZONE = "America/New_York"

SENSOR_IDS = [
    673, 1097, 1102, 1103,
    1152, 1128, 1145, 1146
]


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


hourly_raw = read_csv("openaq_hourly_measurements.csv")
sensor_raw = read_csv("openaq_sensor_metadata.csv")
location_raw = read_csv("openaq_location_metadata.csv")
flags_raw = read_csv("openaq_sensor_flags.csv")

print("Bronze source files loaded.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 2: Standardize hourly measurements and classify data quality


def parse_iso_timestamp(column_name):
    column = F.col(column_name)

    return F.coalesce(
        F.to_timestamp(column, "yyyy-MM-dd HH:mm:ssXXX"),
        F.to_timestamp(column, "yyyy-MM-dd'T'HH:mm:ssX")
    )


hourly_parsed = (
    hourly_raw
    .withColumn("sensor_id", F.col("sensor_id").cast("long"))
    .filter(F.col("sensor_id").isin(SENSOR_IDS))
    .withColumn("timestamp_utc", parse_iso_timestamp("datetime_from_utc"))
    .withColumn("timestamp_end_utc", parse_iso_timestamp("datetime_to_utc"))
    .withColumn("pm25_value", F.col("value").cast("double"))
    .withColumn("parameter", F.lower(F.trim(F.col("parameter"))))
    .withColumn(
        "unit",
        F.regexp_replace(F.trim(F.col("unit")), "μ", "µ")
    )
    .withColumn("period_label", F.lower(F.trim(F.col("period_label"))))
    .withColumn(
        "has_flags",
        F.lower(
            F.trim(F.coalesce(F.col("has_flags"), F.lit("false")))
        ).isin("true", "1", "yes")
    )
)

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

# Derive New York local calendar fields from UTC timestamps.
hourly = (
    hourly_parsed
    .withColumn(
        "local_timestamp",
        F.from_utc_timestamp(F.col("timestamp_utc"), LOCAL_TIMEZONE)
    )
    .withColumn(
        "local_datetime",
        F.date_format(F.col("local_timestamp"), "yyyy-MM-dd HH:mm:ss")
    )
    .withColumn("local_date", F.to_date("local_timestamp"))
    .withColumn("local_hour", F.hour("local_timestamp"))
    .filter(
        (F.col("local_date") >= F.to_date(F.lit(START_LOCAL_DATE)))
        & (
            F.col("local_date")
            < F.to_date(F.lit(END_LOCAL_DATE_EXCLUSIVE))
        )
    )
)

# Assign one mutually exclusive quality status to each hourly record.
hourly = hourly.withColumn(
    "quality_status",
    F.when(F.col("pm25_value").isNull(), "MISSING_VALUE")
    .when(F.col("has_flags"), "FLAGGED")
    .when(F.col("pm25_value") < 0, "NEGATIVE_VALUE")
    .when(
        F.coalesce(F.col("parameter"), F.lit("")) != "pm25",
        "UNEXPECTED_PARAMETER"
    )
    .when(
        F.coalesce(F.col("unit"), F.lit("")) != "µg/m³",
        "UNEXPECTED_UNIT"
    )
    .when(
        F.coalesce(F.col("period_label"), F.lit("")) != "1hour",
        "UNEXPECTED_PERIOD"
    )
    .otherwise("VALID")
).cache()

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
    raise ValueError(
        f"Found {duplicate_count:,} duplicate sensor-hour groups. "
        "Resolve duplicates before writing Silver."
    )

valid_hourly = hourly.filter(F.col("quality_status") == "VALID")
quarantined_hourly = hourly.filter(F.col("quality_status") != "VALID")

print(f"Hourly records in project period: {hourly_record_count:,}")
print(f"Accepted hourly records: {valid_hourly.count():,}")
print(f"Quarantined hourly records: {quarantined_hourly.count():,}")

display(
    hourly
    .groupBy("quality_status")
    .count()
    .orderBy("quality_status")
)

# The original Bronze extraction may end at 2026-04-01 00:00 UTC.
# Complete local March 31 coverage can require hourly records through 03:00 UTC.
source_max_utc = hourly_parsed.agg(
    F.max("timestamp_utc").alias("max_timestamp_utc")
).first()["max_timestamp_utc"]

required_final_hour_utc = datetime(2026, 4, 1, 3, 0, 0)

if source_max_utc is not None and source_max_utc < required_final_hour_utc:
    print(
        "WARNING: The latest selected-sensor Bronze timestamp is "
        f"{source_max_utc}. Complete local March 31 hourly coverage may require "
        "records through 2026-04-01 03:00:00 UTC. The Silver transformation "
        "cannot create source observations that are absent from Bronze."
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 3: Build and validate the Silver sensor dimension

locations = (
    location_raw
    .withColumn("sensor_id", F.col("sensor_id").cast("long"))
    .withColumn("location_id", F.col("location_id").cast("long"))
    .withColumn("latitude", F.col("latitude").cast("double"))
    .withColumn("longitude", F.col("longitude").cast("double"))
    .withColumn(
        "is_monitor",
        F.lower(F.trim(F.coalesce(F.col("is_monitor"), F.lit("false"))))
        .isin("true", "1", "yes")
    )
    .withColumn(
        "is_mobile",
        F.lower(F.trim(F.coalesce(F.col("is_mobile"), F.lit("false"))))
        .isin("true", "1", "yes")
    )
    .withColumn(
        "instrument_name",
        F.get_json_object(F.col("instruments"), "$[0].name")
    )
    .filter(F.col("sensor_id").isin(SENSOR_IDS))
)

sensors = (
    sensor_raw
    .withColumn("sensor_id", F.col("sensor_id").cast("long"))
    .filter(F.col("sensor_id").isin(SENSOR_IDS))
    .select(
        "sensor_id",
        "first_observation_utc",
        "last_observation_utc"
    )
)

expected_sensor_ids = spark.createDataFrame(
    [(sensor_id,) for sensor_id in SENSOR_IDS],
    ["sensor_id"]
)

location_duplicates = (
    locations
    .groupBy("sensor_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

sensor_metadata_duplicates = (
    sensors
    .groupBy("sensor_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

missing_location_ids = [
    row["sensor_id"]
    for row in expected_sensor_ids.join(
        locations.select("sensor_id").distinct(),
        on="sensor_id",
        how="left_anti"
    ).collect()
]

if location_duplicates > 0 or sensor_metadata_duplicates > 0:
    raise ValueError(
        "Duplicate sensor metadata found. "
        f"Location duplicates: {location_duplicates}; "
        f"sensor metadata duplicates: {sensor_metadata_duplicates}."
    )

if missing_location_ids:
    raise ValueError(
        f"Missing location metadata for selected sensors: {missing_location_ids}"
    )

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
        "licenses"
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
        f"Expected {len(SENSOR_IDS)} sensor records, "
        f"found {silver_sensor.count()}."
    )

print(f"Validated Silver sensor records: {silver_sensor.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 4: Enrich accepted hourly measurements with sensor metadata

unmatched_hourly_sensors = (
    valid_hourly
    .select("sensor_id")
    .distinct()
    .join(
        silver_sensor.select("sensor_id"),
        on="sensor_id",
        how="left_anti"
    )
    .count()
)

if unmatched_hourly_sensors > 0:
    raise ValueError(
        f"Found {unmatched_hourly_sensors} sensor IDs in hourly data "
        "without matching sensor metadata."
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
    "longitude"
)

h = valid_hourly.alias("h")
s = sensor_lookup.alias("s")

silver_hourly = (
    h.join(
        s,
        F.col("h.sensor_id") == F.col("s.sensor_id"),
        "inner"
    )
    .select(
        F.col("h.sensor_id"),
        F.col("s.location_id"),
        F.coalesce(
            F.col("s.location_name"),
            F.col("h.location_name")
        ).alias("location_name"),
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
        F.col("h.pm25_value"),
        F.col("h.unit"),
        F.col("h.parameter"),
        F.col("h.has_flags"),
        F.col("h.summary_min").cast("double").alias("within_hour_min"),
        F.col("h.summary_max").cast("double").alias("within_hour_max"),
        F.col("h.summary_mean").cast("double").alias("within_hour_mean"),
        F.col("h.coverage_percent_complete").cast("double")
            .alias("source_hour_coverage_percent")
    )
)

print(f"Prepared accepted Silver hourly records: {silver_hourly.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 5: Prepare OpenAQ flag events for the project period

flags_parsed = (
    flags_raw
    .withColumn("queried_sensor_id", F.col("queried_sensor_id").cast("long"))
    .filter(F.col("queried_sensor_id").isin(SENSOR_IDS))
    .withColumn("flag_start_utc", parse_iso_timestamp("datetime_from_utc"))
    .withColumn("flag_end_utc", parse_iso_timestamp("datetime_to_utc"))
)

invalid_flag_starts = flags_parsed.filter(
    F.col("flag_start_utc").isNull()
).count()

invalid_nonempty_flag_ends = flags_parsed.filter(
    F.col("datetime_to_utc").isNotNull()
    & (F.trim(F.col("datetime_to_utc")) != "")
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
    .withColumn(
        "flag_start_local",
        F.from_utc_timestamp(F.col("flag_start_utc"), LOCAL_TIMEZONE)
    )
    .withColumn(
        "flag_end_local",
        F.when(
            F.col("flag_end_utc").isNotNull(),
            F.from_utc_timestamp(F.col("flag_end_utc"), LOCAL_TIMEZONE)
        )
    )
)

project_start_local = F.to_timestamp(
    F.lit(f"{START_LOCAL_DATE} 00:00:00")
)
project_end_local = F.to_timestamp(
    F.lit(f"{END_LOCAL_DATE_EXCLUSIVE} 00:00:00")
)

# Retain flag intervals that overlap the project period, including events
# that began before November 1 but remained active during the project.
flags = (
    flags_with_local_times
    .filter(F.col("flag_start_local") < project_end_local)
    .filter(
        F.col("flag_end_local").isNull()
        | (F.col("flag_end_local") >= project_start_local)
    )
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
        "note"
    )
)

print(f"Prepared OpenAQ flag events: {flags.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 6: Validate Silver outputs and review monthly coverage

# Confirm accepted and quarantined records are mutually exclusive.
overlapping_hourly_records = (
    silver_hourly
    .select("sensor_id", "timestamp_utc")
    .join(
        quarantined_hourly.select("sensor_id", "timestamp_utc"),
        on=["sensor_id", "timestamp_utc"],
        how="inner"
    )
    .count()
)

accepted_flagged_records = silver_hourly.filter(
    F.col("has_flags") == True
).count()

invalid_accepted_records = silver_hourly.filter(
    F.col("pm25_value").isNull()
    | (F.col("pm25_value") < 0)
    | (F.col("parameter") != "pm25")
    | (F.col("unit") != "µg/m³")
).count()

invalid_quarantined_records = quarantined_hourly.filter(
    F.col("quality_status") == "VALID"
).count()

if any([
    overlapping_hourly_records,
    accepted_flagged_records,
    invalid_accepted_records,
    invalid_quarantined_records
]):
    raise ValueError(
        "Silver output validation failed. "
        f"Overlapping records: {overlapping_hourly_records}; "
        f"flagged accepted records: {accepted_flagged_records}; "
        f"invalid accepted records: {invalid_accepted_records}; "
        f"valid records in quarantine: {invalid_quarantined_records}."
    )

# Confirm the project includes November observations.
all_measurements = (
    silver_hourly
    .select("sensor_id", "timestamp_utc", "local_date")
    .unionByName(
        quarantined_hourly.select("sensor_id", "timestamp_utc", "local_date")
    )
)

november_records = all_measurements.filter(
    F.col("local_date").between("2025-11-01", "2025-11-30")
).count()

if november_records == 0:
    raise ValueError(
        "No November 2025 records were found for the selected sensors. "
        "Check the Bronze source file and the local-date filter."
    )

print(f"November 2025 records available: {november_records:,}")
print(f"Accepted hourly records: {silver_hourly.count():,}")
print(f"Quarantined hourly records: {quarantined_hourly.count():,}")
print(f"Sensor dimension records: {silver_sensor.count():,}")
print(f"Flag events in project period: {flags.count():,}")

# Monthly coverage across accepted and quarantined observations.
display(
    all_measurements
    .groupBy(
        "sensor_id",
        F.date_format("local_date", "yyyy-MM").alias("month")
    )
    .agg(
        F.countDistinct("local_date").alias("days_with_records"),
        F.countDistinct("timestamp_utc").alias("observed_hours")
    )
    .orderBy("sensor_id", "month")
)

# Quarantine breakdown for auditability.
display(
    quarantined_hourly
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

# Cell 7: Write the validated Silver tables


def write_delta(df, table_name):
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"dbo.{table_name}")
    )

    print(f"Written: dbo.{table_name}")


write_delta(silver_hourly, "silver_air_quality_hourly")
write_delta(quarantined_hourly, "silver_air_quality_quarantine")
write_delta(silver_sensor, "silver_air_quality_sensor")
write_delta(flags, "silver_air_quality_flags")

print("Silver transformation completed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 8: Verify the saved Silver tables

saved_hourly = spark.read.table("dbo.silver_air_quality_hourly")
saved_quarantine = spark.read.table("dbo.silver_air_quality_quarantine")
saved_sensor = spark.read.table("dbo.silver_air_quality_sensor")
saved_flags = spark.read.table("dbo.silver_air_quality_flags")

saved_counts = {
    "silver_air_quality_hourly": saved_hourly.count(),
    "silver_air_quality_quarantine": saved_quarantine.count(),
    "silver_air_quality_sensor": saved_sensor.count(),
    "silver_air_quality_flags": saved_flags.count()
}

source_counts = {
    "silver_air_quality_hourly": silver_hourly.count(),
    "silver_air_quality_quarantine": quarantined_hourly.count(),
    "silver_air_quality_sensor": silver_sensor.count(),
    "silver_air_quality_flags": flags.count()
}

for table_name, expected_count in source_counts.items():
    actual_count = saved_counts[table_name]
    print(f"{table_name}: {actual_count:,} records")

    if actual_count != expected_count:
        raise ValueError(
            f"Saved table row-count mismatch for {table_name}: "
            f"expected {expected_count}, found {actual_count}."
        )

saved_measurements = (
    saved_hourly
    .select("sensor_id", "timestamp_utc", "local_date")
    .unionByName(
        saved_quarantine.select("sensor_id", "timestamp_utc", "local_date")
    )
)

saved_date_bounds = saved_measurements.agg(
    F.min("local_date").alias("first_local_date"),
    F.max("local_date").alias("last_local_date"),
    F.countDistinct("local_date").alias("distinct_local_dates"),
    F.countDistinct("sensor_id").alias("distinct_sensors")
).first()

print("First local date:", saved_date_bounds["first_local_date"])
print("Last local date:", saved_date_bounds["last_local_date"])
print("Distinct local dates:", saved_date_bounds["distinct_local_dates"])
print("Distinct sensors:", saved_date_bounds["distinct_sensors"])

if saved_date_bounds["distinct_sensors"] != len(SENSOR_IDS):
    raise ValueError(
        "The saved Silver measurements do not contain all selected sensors."
    )

if saved_date_bounds["first_local_date"] is None:
    raise ValueError("The saved Silver measurement tables are empty.")

print("All saved Silver tables passed row-count and date-range validation.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
