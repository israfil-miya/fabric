# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "43446074-db34-4162-b6f2-3289971e64a2",
# META       "default_lakehouse_name": "lh_bronze",
# META       "default_lakehouse_workspace_id": "14530c94-9dc4-4ef1-a320-081555064f5d",
# META       "known_lakehouses": [
# META         {
# META           "id": "43446074-db34-4162-b6f2-3289971e64a2"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Taxi Medallion Transformation
# 
# **Purpose**  
# Transform NYC Yellow Taxi data.
# 
# **Source**  
# - `Files/taxi/`
# 
# **Outputs**  
# - `dbo.silver_taxi`
# - `dbo.dim_date`
# - `dbo.fact_taxi_daily`

# MARKDOWN ********************

# ## 1. Silver Layer
# Clean the raw data, apply quality filters, and add useful flags.

# CELL ********************

from pyspark.sql import functions as F

target = "dbo.silver_taxi"

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
    raw_in = spark.read.parquet(f"Files/taxi/yellow_tripdata_{year_month}.parquet")

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

    if spark.catalog.tableExists(target):
        (silver.write.mode("overwrite").format("delta")
               .option("replaceWhere", f"year_month = '{year_month}'")
               .saveAsTable(target))
    else:
        silver.write.format("delta").saveAsTable(target)

    raw_n = raw.count()
    written = spark.table(target).filter(F.col("year_month") == year_month).count()
    assert written > 0, f"nothing written for {year_month}"
    return (year_month,
            f"in_type={in_type}  raw={raw_n:,}  written={written:,}  dropped={raw_n - written:,}")


# --- the only thing you edit ---
months = ["2026-01-01", "2026-02-01"]

results = []
for ms in months:
    try:
        results.append(load_month(ms))
    except Exception as e:
        results.append((ms[:7], f"FAILED: {str(e)[:300]}"))

print("---- summary ----")
for ym, msg in results:
    print(ym, msg)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. DimDate
# Date dimension covering 2024–2027. Used by all fact tables.

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from datetime import date, timedelta

start = date(2024, 1, 1)
end   = date(2027, 12, 31)
days  = [(start + timedelta(days=i),) for i in range((end - start).days + 1)]

df = spark.createDataFrame(days, ["full_date"]).withColumn("full_date", F.col("full_date").cast(DateType()))

dim_date = (
    df
    .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("full_date"))
    .withColumn("quarter", F.quarter("full_date"))
    .withColumn("month", F.month("full_date"))
    .withColumn("month_name", F.date_format("full_date", "MMMM"))
    .withColumn("day", F.dayofmonth("full_date"))
    .withColumn("day_of_week", F.dayofweek("full_date"))
    .withColumn("day_name", F.date_format("full_date", "EEEE"))
    .withColumn("is_weekend", F.dayofweek("full_date").isin([1, 7]))
    .withColumn("year_month", F.date_format("full_date", "yyyy-MM"))
    .select(
        "date_key", "full_date", "year", "quarter", "month", "month_name",
        "day", "day_of_week", "day_name", "is_weekend", "year_month"
    )
    .orderBy("date_key")
)

print(f"rows: {dim_date.count()}")
dim_date.write.mode("overwrite").format("delta").saveAsTable("dbo.dim_date")
print("wrote dbo.dim_date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. FactTaxiDaily
# Star-schema daily fact table with date_key foreign key.

# CELL ********************

from pyspark.sql import functions as F

target = "dbo.fact_taxi_daily"
months = ["2026-01-01", "2026-02-01"]   # add new months here

def next_month(ms):
    y, m = int(ms[:4]), int(ms[5:7])
    return f"{y + (m == 12)}-{(m % 12) + 1:02d}-01"

for month_start in months:
    month_end  = next_month(month_start)
    year_month = month_start[:7]

    silver = spark.table("dbo.silver_taxi").filter(F.col("year_month") == year_month)

    fact = (
        silver
        .withColumn("date_key", F.date_format("pickup_date", "yyyyMMdd").cast("int"))
        .groupBy("date_key")
        .agg(
            F.count("*").alias("trip_count"),
            F.count(F.when(~F.col("is_negative_amount"), True)).alias("revenue_trip_count"),
            F.round(F.sum(F.when(~F.col("is_negative_amount"), F.col("fare_amount"))), 2).alias("total_fare_usd"),
            F.round(F.sum(F.when(~F.col("is_negative_amount"), F.col("total_amount"))), 2).alias("total_amount_usd"),
            F.round(F.avg(F.when(~F.col("is_zero_distance"), F.col("trip_distance"))), 2).alias("avg_distance_miles"),
            F.round(F.avg("trip_duration_min"), 1).alias("avg_duration_min"),
            F.sum(F.col("is_zero_distance").cast("int")).alias("zero_distance_trips"),
            F.sum(F.col("is_negative_amount").cast("int")).alias("negative_amount_trips"),
            F.sum(F.col("is_passenger_missing").cast("int")).alias("missing_passenger_trips"),
        )
    )

    lo = int(month_start.replace("-", ""))
    hi = int(month_end.replace("-", ""))
    (fact.write.mode("overwrite").format("delta")
         .option("replaceWhere", f"date_key >= {lo} AND date_key < {hi}")
         .saveAsTable(target))
    print(f"{year_month}: done")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Quick Validation

# CELL ********************

from pyspark.sql import functions as F

fact = spark.table("dbo.fact_taxi_daily")
dim  = spark.table("dbo.dim_date")

print("fact dates missing from dim_date:", fact.join(dim, "date_key", "left_anti").count())
print("duplicate date_keys in dim_date:", dim.count() - dim.select("date_key").distinct().count())
print("dim_date range:", dim.agg(F.min("full_date"), F.max("full_date")).first())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
