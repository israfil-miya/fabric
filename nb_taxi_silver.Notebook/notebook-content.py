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
# META           "id": "43446074-db34-4162-b6f2-3289971e64a2"
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
# The pipeline passes the resolved month list as JSON. Set MONTHS_JSON when running interactively.

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

# MARKDOWN ********************

# ## Silver Layer
# Clean the raw data, apply quality filters, and add useful flags.

# CELL ********************

from pyspark.sql import functions as F

target = "dbo.silver_taxi"

BRONZE_TAXI_PATH = "abfss://nyc_analytics@onelake.dfs.fabric.microsoft.com/lh_bronze.Lakehouse/Files/taxi"

SCHEMA = [
    ("VendorID", "int"),
    ("tpep_pickup_datetime", "timestamp_ntz"),
    ("tpep_dropoff_datetime", "timestamp_ntz"),
    ("passenger_count", "bigint"),
    ("trip_distance", "double"),
    ("RatecodeID", "bigint"),
    ("store_and_fwd_flag", "string"),
    ("PULocationID", "int"),
    ("DOLocationID", "int"),
    ("payment_type", "bigint"),
    ("fare_amount", "double"),
    ("extra", "double"),
    ("mta_tax", "double"),
    ("tip_amount", "double"),
    ("tolls_amount", "double"),
    ("improvement_surcharge", "double"),
    ("total_amount", "double"),
    ("congestion_surcharge", "double"),
    ("Airport_fee", "double"),
    ("cbd_congestion_fee", "double"),
]

REQUIRED = {"tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance",
            "fare_amount", "total_amount", "PULocationID"}


def next_month(ms):
    """First day of the month after ms (exclusive end date)."""
    y, m = int(ms[:4]), int(ms[5:7])
    return f"{y + (m == 12)}-{(m % 12) + 1:02d}-01"


def load_month(month_start):
    year_month = month_start[:7]
    month_end = next_month(month_start)
    raw_in = spark.read.parquet(
        f"{BRONZE_TAXI_PATH}/yellow_tripdata_{year_month}.parquet"
    )

    # incoming type of the pickup column, for the summary line
    in_type = dict(raw_in.dtypes).get("tpep_pickup_datetime", "missing")

    # case-insensitive column matching; fail early if a required column is absent
    lower = {c.lower(): c for c in raw_in.columns}
    missing_req = {c for c in REQUIRED if c.lower() not in lower}
    assert not missing_req, f"missing required columns: {missing_req}"

    # fixed schema: cast every column, add nulls for columns this month lacks
    raw = raw_in.select(*[
        (F.col(lower[n.lower()]).cast(t) if n.lower() in lower
         else F.lit(None).cast(t)).alias(n)
        for n, t in SCHEMA
    ])

    silver = (
        raw
        .filter(
            (F.col("tpep_pickup_datetime") >= F.lit(month_start).cast("timestamp_ntz")) &
            (F.col("tpep_pickup_datetime") <  F.lit(month_end).cast("timestamp_ntz")) &
            (F.col("tpep_dropoff_datetime") >= F.col("tpep_pickup_datetime")) &
            (F.col("trip_distance") <= 100) &
            ((F.unix_timestamp("tpep_dropoff_datetime")
              - F.unix_timestamp("tpep_pickup_datetime")) / 60.0 <= 480)
        )
        .withColumn("year_month", F.lit(year_month))
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .withColumn("trip_duration_min",
            (F.unix_timestamp("tpep_dropoff_datetime")
             - F.unix_timestamp("tpep_pickup_datetime")) / 60.0)
        .withColumn("is_negative_amount",
            (F.col("fare_amount") < 0) | (F.col("total_amount") < 0))
        .withColumn("is_zero_distance", F.col("trip_distance") == 0)
        .withColumn("is_passenger_missing", F.col("passenger_count").isNull())
    )

    # guard: nothing outside the month may be written
    bad = silver.filter(
        (F.col("pickup_date") < month_start) | (F.col("pickup_date") >= month_end)
    ).count()
    assert bad == 0, f"{bad} rows fall outside {year_month}"

    raw_n = raw.count()
    if raw_n == 0:
        raise ValueError(f"source Parquet file for {year_month} is empty")
    silver_n = silver.count()
    if silver_n == 0:
        raise ValueError(f"quality filters produced no rows for {year_month}")

    if spark.catalog.tableExists(target):
        (silver.write.mode("overwrite").format("delta")
               .option("replaceWhere", f"year_month = '{year_month}'")
               .saveAsTable(target))
    else:
        silver.write.format("delta").saveAsTable(target)

    written = spark.table(target).filter(F.col("year_month") == year_month).count()
    if written != silver_n:
        raise ValueError(
            f"Silver count mismatch for {year_month}: expected {silver_n}, wrote {written}"
        )
    return (year_month,
            f"in_type={in_type}  raw={raw_n:,}  written={written:,}  dropped={raw_n - written:,}")


results = []
for ym in MONTHS:
    results.append(load_month(f"{ym}-01"))

print("---- summary ----")
for ym, msg in results:
    print(ym, msg)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
