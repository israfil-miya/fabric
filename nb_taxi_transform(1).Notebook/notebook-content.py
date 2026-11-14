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

# ## 0. Configuration
# Edit `MONTHS` here only. Every later cell reads it. Run this cell first in a new session.

# CELL ********************

import re

# Months to process, as "YYYY-MM". Each needs Files/taxi/yellow_tripdata_YYYY-MM.parquet
# (the same values you give the pipeline's `months` parameter).
MONTHS = ["2025-11", "2025-12", "2026-01", "2026-02", "2026-03"]

assert all(re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", m) for m in MONTHS), \
    "MONTHS must be 'YYYY-MM' strings"
print("months to process:", MONTHS)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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


results = []
for ym in MONTHS:
    try:
        results.append(load_month(f"{ym}-01"))
    except Exception as e:
        results.append((ym, f"FAILED: {str(e)[:300]}"))

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
# Months come from the `MONTHS` constant in the configuration cell. A month with no Silver rows is skipped.
# Includes a payment type 0 (Flex Fare) slice so averages can be computed with and without it.

# CELL ********************

from pyspark.sql import functions as F

target = "dbo.fact_taxi_daily"

def next_month(ms):
    y, m = int(ms[:4]), int(ms[5:7])
    return f"{y + (m == 12)}-{(m % 12) + 1:02d}-01"

def sum_if(cond, col):
    """Sum of col over rows matching cond, 0.0 (not null) when none match, rounded to cents."""
    return F.round(F.coalesce(F.sum(F.when(cond, F.col(col))), F.lit(0.0)), 2)

is_flex = F.col("payment_type") == 0   # TLC "Flex Fare trip"

for month_start in [f"{ym}-01" for ym in MONTHS]:
    month_end  = next_month(month_start)
    year_month = month_start[:7]

    silver = spark.table("dbo.silver_taxi").filter(F.col("year_month") == year_month)
    if silver.limit(1).count() == 0:
        print(f"{year_month}: no rows in silver, skipped")
        continue

    fact = (
        silver
        .withColumn("date_key", F.date_format("pickup_date", "yyyyMMdd").cast("int"))
        .groupBy("date_key")
        .agg(
            F.count("*").alias("trip_count"),
            F.count(F.when(F.col("total_amount") >= 0, True)).alias("revenue_trip_count"),
            F.round(F.sum(F.when(~F.col("is_negative_amount"), F.col("fare_amount"))), 2).alias("total_fare_usd"),
            F.round(F.sum(F.when(~F.col("is_negative_amount"), F.col("total_amount"))), 2).alias("total_amount_usd"),
            F.round(F.sum("fare_amount"), 2).alias("net_fare_usd"),
            F.round(F.sum("total_amount"), 2).alias("net_total_amount_usd"),
            F.round(F.avg(F.when(~F.col("is_zero_distance"), F.col("trip_distance"))), 2).alias("avg_distance_miles"),
            F.round(F.avg("trip_duration_min"), 1).alias("avg_duration_min"),
            F.sum(F.col("is_zero_distance").cast("int")).alias("zero_distance_trips"),
            F.sum(F.col("is_negative_amount").cast("int")).alias("negative_amount_trips"),
            F.sum(F.col("is_passenger_missing").cast("int")).alias("missing_passenger_trips"),
            # payment type 0 (Flex Fare) slice, so averages can be computed with and without it
            F.count(F.when(is_flex, True)).alias("flex_fare_trip_count"),
            sum_if(is_flex, "fare_amount").alias("net_fare_flex_usd"),
            sum_if(is_flex, "total_amount").alias("net_total_amount_flex_usd"),
        )
    )

    lo = int(month_start.replace("-", ""))
    hi = int(month_end.replace("-", ""))

    if spark.catalog.tableExists(target):
        (fact.write.mode("overwrite").format("delta")
             .option("replaceWhere", f"date_key >= {lo} AND date_key < {hi}")
             .option("mergeSchema", "true")
             .saveAsTable(target))
    else:
        fact.write.format("delta").saveAsTable(target)

    print(f"{year_month}: done")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Validation
# Reconciles the fact table against Silver for every month in `MONTHS`; the last line must read ALL CHECKS PASSED.

# CELL ********************

from pyspark.sql import functions as F

fact   = spark.table("dbo.fact_taxi_daily")
dim    = spark.table("dbo.dim_date")
silver = spark.table("dbo.silver_taxi")
problems = []

# ---- 1. per-month reconciliation: fact vs silver ----
fact_m = (fact.join(dim, "date_key").groupBy("year_month")
          .agg(F.count("*").alias("fact_days"),
               F.sum("trip_count").alias("fact_trips"),
               F.sum("revenue_trip_count").alias("fact_rtc"),
               F.sum("flex_fare_trip_count").alias("fact_pt0"),
               F.round(F.sum("net_total_amount_usd"), 2).alias("fact_net_total")))

silver_m = (silver.groupBy("year_month")
            .agg(F.count("*").alias("silver_trips"),
                 F.sum((F.col("total_amount") >= 0).cast("int")).alias("silver_rtc"),
                 F.sum((F.col("payment_type") == 0).cast("int")).alias("silver_pt0"),
                 F.round(F.sum("total_amount"), 2).alias("silver_net_total")))

dim_m = dim.groupBy("year_month").agg(F.count("*").alias("calendar_days"))

recon = (silver_m.join(fact_m, "year_month", "full_outer")
         .join(dim_m, "year_month", "left")
         .withColumn("trips_diff",     F.col("silver_trips") - F.col("fact_trips"))
         .withColumn("rtc_diff",       F.col("silver_rtc")   - F.col("fact_rtc"))
         .withColumn("pt0_diff",       F.col("silver_pt0")   - F.col("fact_pt0"))
         .withColumn("net_total_diff", F.round(F.col("silver_net_total") - F.col("fact_net_total"), 2))
         .withColumn("days_diff",      F.col("calendar_days") - F.col("fact_days"))
         .orderBy("year_month"))

recon.select("year_month", "silver_trips", "fact_trips", "trips_diff", "rtc_diff",
             "pt0_diff", "net_total_diff", "fact_days", "calendar_days", "days_diff").show(truncate=False)

bad_months = recon.filter(
    F.col("trips_diff").isNull() | (F.col("trips_diff") != 0) |
    (F.col("rtc_diff") != 0) | (F.col("pt0_diff") != 0) |
    (F.abs(F.col("net_total_diff")) > 1.0) |
    F.col("days_diff").isNull() | (F.col("days_diff") != 0)).count()
if bad_months:
    problems.append(f"{bad_months} month(s) fail reconciliation")

# ---- 2. table-level checks ----
n_fact = fact.count()
print("fact rows:", n_fact)

missing_dates = fact.join(dim, "date_key", "left_anti").count()
dup_fact      = n_fact - fact.select("date_key").distinct().count()
dup_dim       = dim.count() - dim.select("date_key").distinct().count()
print("fact dates missing from dim_date:", missing_dates)
print("duplicate date_keys in fact:", dup_fact)
print("duplicate date_keys in dim_date:", dup_dim)
for name, v in [("dates missing from dim_date", missing_dates),
                ("duplicate date_keys in fact", dup_fact),
                ("duplicate date_keys in dim_date", dup_dim)]:
    if v:
        problems.append(f"{name}: {v}")

must_have = ["trip_count", "revenue_trip_count", "net_fare_usd", "net_total_amount_usd",
             "flex_fare_trip_count", "net_fare_flex_usd", "net_total_amount_flex_usd"]
null_rows = fact.filter(F.expr(" OR ".join(f"{c} IS NULL" for c in must_have))).count()
print("rows with null in required columns:", null_rows)
if null_rows:
    problems.append(f"{null_rows} fact rows have nulls in required columns")

print("dim_date range:", dim.agg(F.min("full_date"), F.max("full_date")).first())

# ---- 3. MONTHS coverage ----
silver_months = {r[0] for r in silver.select("year_month").distinct().collect()}
fact_months   = {r[0] for r in fact.join(dim, "date_key").select("year_month").distinct().collect()}
miss_silver = sorted(set(MONTHS) - silver_months)
miss_fact   = sorted(set(MONTHS) - fact_months)
print("MONTHS missing from silver:", miss_silver)
print("MONTHS missing from fact:  ", miss_fact)
if miss_silver: problems.append(f"missing from silver: {miss_silver}")
if miss_fact:   problems.append(f"missing from fact: {miss_fact}")

print("\nALL CHECKS PASSED" if not problems else "\nPROBLEMS: " + "; ".join(problems))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
