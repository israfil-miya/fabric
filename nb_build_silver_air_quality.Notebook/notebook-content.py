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


from pyspark.sql import functions as F

# Keep timestamp parsing consistent.
spark.conf.set("spark.sql.session.timeZone", "UTC")

# Paste the ABFS path copied from the Bronze Lakehouse.
HOURLY_ABFS_PATH = (
    "abfss://nyc_analytics@onelake.dfs.fabric.microsoft.com/lh_bronze.Lakehouse/Files/air_quality/openaq/"
)

SOURCE_DIR = HOURLY_ABFS_PATH.rsplit("/", 1)[0]

SENSOR_IDS = [
    673, 1097, 1102, 1103,
    1152, 1128, 1145, 1146
]


def read_csv(filename):
    path = f"{SOURCE_DIR}/{filename}"

    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("multiLine", "true")
        .option("escape", '"')
        .load(path)
    )


# Read Bronze data.
hourly_raw = read_csv(
    "openaq_hourly_measurements.csv"
)

sensor_raw = read_csv(
    "openaq_sensor_metadata.csv"
)

location_raw = read_csv(
    "openaq_location_metadata.csv"
)

flags_raw = read_csv(
    "openaq_sensor_flags.csv"
)


print("Hourly records:", hourly_raw.count())
print("Sensor metadata:", sensor_raw.count())
print("Location metadata:", location_raw.count())
print("Flag records:", flags_raw.count())

display(hourly_raw.limit(5))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


def parse_iso_timestamp(column_name):
    column = F.col(column_name)

    return F.coalesce(
        F.to_timestamp(
            column,
            "yyyy-MM-dd HH:mm:ssXXX"
        ),
        F.to_timestamp(
            column,
            "yyyy-MM-dd'T'HH:mm:ssX"
        )
    )


# -----------------------------------------------------
# 1. Standardize column types
# -----------------------------------------------------

hourly = (
    hourly_raw

    .withColumn(
        "sensor_id",
        F.col("sensor_id").cast("long")
    )

    .filter(
        F.col("sensor_id").isin(SENSOR_IDS)
    )

    .withColumn(
        "timestamp_utc",
        parse_iso_timestamp("datetime_from_utc")
    )

    .withColumn(
        "timestamp_end_utc",
        parse_iso_timestamp("datetime_to_utc")
    )

    .withColumn(
        "pm25_value",
        F.col("value").cast("double")
    )

    .withColumn(
        "parameter",
        F.lower(F.trim(F.col("parameter")))
    )

    .withColumn(
        "unit",
        F.trim(F.col("unit"))
    )

    .withColumn(
        "period_label",
        F.trim(F.col("period_label"))
    )

    .withColumn(
        "has_flags",
        F.lower(
            F.trim(
                F.coalesce(
                    F.col("has_flags"),
                    F.lit("false")
                )
            )
        ).isin("true", "1", "yes")
    )

    .withColumn(
        "local_timestamp",
        F.from_utc_timestamp(
            F.col("timestamp_utc"),
            "America/New_York"
        )
    )

    .withColumn(
        "local_datetime",
        F.date_format(
            F.col("local_timestamp"),
            "yyyy-MM-dd HH:mm:ss"
        )
    )

    .withColumn(
        "local_date",
        F.to_date("local_timestamp")
    )

    .withColumn(
        "local_hour",
        F.hour("local_timestamp")
    )
)


# Check timestamp parsing before applying the date filter.
unparsed_timestamps = hourly.filter(
    F.col("timestamp_utc").isNull()
)

if unparsed_timestamps.count() > 0:
    display(unparsed_timestamps)
    raise ValueError(
        "Some UTC timestamps could not be parsed."
    )


# -----------------------------------------------------
# 2. Filter the project period using NYC local dates
# -----------------------------------------------------

hourly = hourly.filter(
    (F.col("local_date") >= F.lit("2025-12-01").cast("date"))
    &
    (F.col("local_date") < F.lit("2026-04-01").cast("date"))
)


# -----------------------------------------------------
# 3. Assign data quality status
# -----------------------------------------------------

normalized_unit = F.lower(
    F.trim(
        F.regexp_replace(
            F.col("unit"),
            "μ",
            "µ"
        )
    )
)

hourly = (
    hourly
    .withColumn(
        "quality_status",

        F.when(
            F.col("pm25_value").isNull(),
            "MISSING_VALUE"
        )

        .when(
            F.col("has_flags"),
            "FLAGGED"
        )

        .when(
            F.col("pm25_value") < 0,
            "NEGATIVE_VALUE"
        )

        .when(
            F.coalesce(
                F.col("parameter"),
                F.lit("")
            ) != "pm25",
            "UNEXPECTED_PARAMETER"
        )

        .when(
            normalized_unit != "µg/m³",
            "UNEXPECTED_UNIT"
        )

        .when(
            F.coalesce(
                F.col("period_label"),
                F.lit("")
            ) != "1hour",
            "UNEXPECTED_PERIOD"
        )

        .otherwise("VALID")
    )
)


# -----------------------------------------------------
# 4. Check duplicate sensor-hour combinations
# -----------------------------------------------------

duplicates = (
    hourly
    .groupBy(
        "sensor_id",
        "timestamp_utc"
    )
    .count()
    .filter(F.col("count") > 1)
)

duplicate_count = duplicates.count()

print("Duplicate sensor-hour groups:", duplicate_count)

if duplicate_count > 0:
    display(duplicates)

    raise ValueError(
        "Duplicate sensor-hour records found. "
        "Inspect them before writing Silver."
    )


# -----------------------------------------------------
# 5. Split valid and rejected records
# -----------------------------------------------------

valid_hourly = hourly.filter(
    F.col("quality_status") == "VALID"
)

quarantined_hourly = hourly.filter(
    F.col("quality_status") != "VALID"
)

print("Total records:", hourly.count())
print("Valid records:", valid_hourly.count())
print("Rejected records:", quarantined_hourly.count())

display(
    hourly.groupBy("quality_status")
    .count()
    .orderBy("quality_status")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# -----------------------------------------------------
# 1. Clean location metadata
# -----------------------------------------------------

locations = (
    location_raw

    .withColumn(
        "sensor_id",
        F.col("sensor_id").cast("long")
    )

    .withColumn(
        "location_id",
        F.col("location_id").cast("long")
    )

    .withColumn(
        "latitude",
        F.col("latitude").cast("double")
    )

    .withColumn(
        "longitude",
        F.col("longitude").cast("double")
    )

    .withColumn(
        "is_monitor",
        F.lower(F.col("is_monitor")).isin("true", "1")
    )

    .withColumn(
        "is_mobile",
        F.lower(F.col("is_mobile")).isin("true", "1")
    )

    .withColumn(
        "instrument_name",
        F.get_json_object(
            F.col("instruments"),
            "$[0].name"
        )
    )

    .filter(
        F.col("sensor_id").isin(SENSOR_IDS)
    )
)


# -----------------------------------------------------
# 2. Clean sensor metadata
# -----------------------------------------------------

sensors = (
    sensor_raw

    .withColumn(
        "sensor_id",
        F.col("sensor_id").cast("long")
    )

    .filter(
        F.col("sensor_id").isin(SENSOR_IDS)
    )

    .select(
        "sensor_id",
        "first_observation_utc",
        "last_observation_utc"
    )
)


# -----------------------------------------------------
# 3. Build sensor dimension
# -----------------------------------------------------

silver_sensor = (
    locations
    .join(
        sensors,
        on="sensor_id",
        how="left"
    )
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
    .dropDuplicates(["sensor_id"])
)


# Ensure the selected sensors are the expected monitoring network.
unexpected_sources = silver_sensor.filter(
    (F.col("provider_name") != "AirNow")
    |
    (F.col("is_monitor") != True)
)

if unexpected_sources.count() > 0:
    display(unexpected_sources)

    raise ValueError(
        "Unexpected provider or monitor type found."
    )


print("Selected sensors:", silver_sensor.count())

display(silver_sensor)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


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
    "timezone"
)

h = valid_hourly.alias("h")
s = sensor_lookup.alias("s")

silver_hourly = (
    h.join(
        s,
        F.col("h.sensor_id") == F.col("s.sensor_id"),
        "left"
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


display(silver_hourly.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


flags = (
    flags_raw

    .withColumn(
        "queried_sensor_id",
        F.col("queried_sensor_id").cast("long")
    )

    .withColumn(
        "flag_start_utc",
        parse_iso_timestamp("datetime_from_utc")
    )

    .withColumn(
        "flag_end_utc",
        parse_iso_timestamp("datetime_to_utc")
    )

    .withColumn(
        "flag_local_date",
        F.to_date(
            F.from_utc_timestamp(
                F.col("flag_start_utc"),
                "America/New_York"
            )
        )
    )

    .filter(
        F.col("queried_sensor_id").isin(SENSOR_IDS)
    )

    .filter(
        (F.col("flag_local_date") >= F.lit("2025-12-01").cast("date"))
        &
        (F.col("flag_local_date") < F.lit("2026-04-01").cast("date"))
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

display(flags)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


def write_delta(df, table_name):
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"dbo.{table_name}")
    )

    print(f"Written: dbo.{table_name}")


write_delta(
    silver_hourly,
    "silver_air_quality_hourly"
)

write_delta(
    quarantined_hourly,
    "silver_air_quality_quarantine"
)

write_delta(
    silver_sensor,
    "silver_air_quality_sensor"
)

write_delta(
    flags,
    "silver_air_quality_flags"
)


print("\nSilver transformation completed.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
