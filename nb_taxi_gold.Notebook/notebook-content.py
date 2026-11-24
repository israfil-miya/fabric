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
# The pipeline passes the resolved month list as JSON. Set MONTHS_JSON when running interactively.

# PARAMETERS CELL ********************

import json
import re

MONTHS_JSON = '["2025-11","2025-12","2026-01","2026-02","2026-03"]'


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

# ## DimDate
# Date dimension covering 2024–2027. Used by all fact tables.

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from datetime import date, timedelta

def next_month_start(month):
    year, month_number = int(month[:4]), int(month[5:7])
    return date(year + (month_number == 12), month_number % 12 + 1, 1)


selected_starts = [date.fromisoformat(f"{month}-01") for month in MONTHS]
selected_ends = [
    next_month_start(month) - timedelta(days=1) for month in MONTHS
]
start = min(selected_starts)
end = max(selected_ends)

if spark.catalog.tableExists("dbo.dim_date"):
    existing_bounds = spark.table("dbo.dim_date").agg(
        F.min("full_date").alias("min_date"),
        F.max("full_date").alias("max_date")
    ).first()
    if existing_bounds["min_date"] is not None:
        start = min(start, existing_bounds["min_date"])
        end = max(end, existing_bounds["max_date"])

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

# ## FactTaxiDaily
# Star-schema daily fact table with date_key foreign key and complete calendar dates per month.
# Months come from the resolved `MONTHS` selection. Requires passed Silver quality audit rows.
# Reports gross nonnegative, signed net, zero-distance, negative/missing amount, and payment-type metrics.

# CELL ********************

from datetime import date, timedelta
from pyspark.sql import functions as F

target = "dbo.fact_taxi_daily"
silver_table = "lh_silver.dbo.silver_taxi"
audit_table = "lh_silver.dbo.silver_taxi_quality_audit"


def next_month(ms):
    y, m = int(ms[:4]), int(ms[5:7])
    return f"{y + (m == 12)}-{(m % 12) + 1:02d}-01"


def sum_if(cond, col):
    """Sum of col over rows matching cond, 0.0 (not null) when none match, rounded to cents."""
    return F.round(F.coalesce(F.sum(F.when(cond, F.col(col))), F.lit(0.0)), 2)


def sum_flag(col_name):
    """Count rows where a boolean quality flag is True."""
    return F.coalesce(F.sum(F.col(col_name).cast("long")), F.lit(0))


# Pre-flight: confirm every selected month passed Silver reconciliation before publishing Gold.
if not spark.catalog.tableExists(audit_table):
    raise ValueError(f"Missing Silver audit table {audit_table}; refusing to publish Gold")

audit_df = spark.table(audit_table).filter(F.col("source_year_month").isin(MONTHS))
audit_by_month = {row["source_year_month"]: row for row in audit_df.collect()}
missing_audit = sorted(set(MONTHS) - set(audit_by_month))
failed_audit = sorted(
    ym for ym, row in audit_by_month.items()
    if not row["reconciliation_passed"] or row["bronze_row_count"] <= 0
)
if missing_audit or failed_audit:
    raise ValueError(
        f"Silver quality audit check failed. Missing months: {missing_audit}; "
        f"failed months: {failed_audit}. Refusing to publish Gold."
    )

is_nonnegative_fare = (~F.col("is_missing_fare")) & (~F.col("is_negative_fare"))
is_nonnegative_total = (~F.col("is_missing_total")) & (~F.col("is_negative_total"))
is_nonnegative_revenue = (~F.col("is_missing_amount")) & (~F.col("is_negative_amount"))
is_valid_reported_distance = (~F.col("is_missing_distance")) & (~F.col("is_negative_distance"))
is_positive_distance = (~F.col("is_missing_distance")) & (F.col("trip_distance") > 0)
is_flex = F.col("payment_type") == 0   # TLC "Flex Fare trip"

COUNT_ZERO_FILL_COLS = [
    "trip_count",
    "revenue_trip_count",
    "nonnegative_fare_trip_count",
    "nonnegative_total_trip_count",
    "zero_distance_trips",
    "missing_distance_trips",
    "negative_distance_trips",
    "unusual_distance_trips",
    "zero_duration_trips",
    "unusual_duration_trips",
    "missing_dropoff_trips",
    "negative_fare_trips",
    "negative_total_trips",
    "negative_amount_trips",
    "missing_fare_trips",
    "missing_total_trips",
    "missing_amount_trips",
    "missing_passenger_trips",
    "missing_pu_location_trips",
    "duplicate_record_trips",
    "flex_fare_trip_count",
    "credit_card_trip_count",
    "cash_trip_count",
    "no_charge_trip_count",
    "dispute_trip_count",
    "unknown_payment_trip_count",
    "voided_trip_count",
    "other_payment_trip_count",
]

AMOUNT_ZERO_FILL_COLS = [
    "total_fare_usd",
    "total_amount_usd",
    "gross_nonnegative_fare_usd",
    "gross_nonnegative_total_amount_usd",
    "net_fare_usd",
    "net_total_amount_usd",
    "net_fare_flex_usd",
    "net_total_amount_flex_usd",
]

for month_start in [f"{ym}-01" for ym in MONTHS]:
    month_end = next_month(month_start)
    year_month = month_start[:7]

    silver = spark.table(silver_table).filter(F.col("year_month") == year_month)
    if silver.limit(1).count() == 0:
        raise ValueError(f"{year_month}: no rows in Silver; refusing to publish Gold")

    start_d = date.fromisoformat(month_start)
    end_d = date.fromisoformat(month_end)
    month_dates = [
        (int((start_d + timedelta(days=i)).strftime("%Y%m%d")),)
        for i in range((end_d - start_d).days)
    ]
    date_grid = spark.createDataFrame(month_dates, ["date_key"])

    daily_agg = (
        silver
        .withColumn("date_key", F.date_format("pickup_date", "yyyyMMdd").cast("int"))
        .groupBy("date_key")
        .agg(
            F.count("*").alias("trip_count"),
            # Revenue trip count and gross revenue share the exact same eligibility rule
            F.count(F.when(is_nonnegative_revenue, True)).alias("revenue_trip_count"),
            sum_if(is_nonnegative_revenue, "fare_amount").alias("total_fare_usd"),
            sum_if(is_nonnegative_revenue, "total_amount").alias("total_amount_usd"),
            # Measure-specific nonnegative gross and signed net totals
            F.count(F.when(is_nonnegative_fare, True)).alias("nonnegative_fare_trip_count"),
            sum_if(is_nonnegative_fare, "fare_amount").alias("gross_nonnegative_fare_usd"),
            F.count(F.when(is_nonnegative_total, True)).alias("nonnegative_total_trip_count"),
            sum_if(is_nonnegative_total, "total_amount").alias("gross_nonnegative_total_amount_usd"),
            sum_if(~F.col("is_missing_fare"), "fare_amount").alias("net_fare_usd"),
            sum_if(~F.col("is_missing_total"), "total_amount").alias("net_total_amount_usd"),
            # Distance: include zero in avg_distance_miles; keep positive-only average as comparison
            F.round(
                F.avg(F.when(is_valid_reported_distance, F.col("trip_distance"))),
                2,
            ).alias("avg_distance_miles"),
            F.round(
                F.avg(F.when(is_positive_distance, F.col("trip_distance"))),
                2,
            ).alias("avg_positive_distance_miles"),
            F.round(F.avg("trip_duration_min"), 1).alias("avg_duration_min"),
            # Quality indicators
            sum_flag("is_zero_distance").alias("zero_distance_trips"),
            sum_flag("is_missing_distance").alias("missing_distance_trips"),
            sum_flag("is_negative_distance").alias("negative_distance_trips"),
            sum_flag("is_unusual_distance").alias("unusual_distance_trips"),
            sum_flag("is_zero_duration").alias("zero_duration_trips"),
            sum_flag("is_unusual_duration").alias("unusual_duration_trips"),
            sum_flag("is_missing_dropoff_datetime").alias("missing_dropoff_trips"),
            sum_flag("is_negative_fare").alias("negative_fare_trips"),
            sum_flag("is_negative_total").alias("negative_total_trips"),
            sum_flag("is_negative_amount").alias("negative_amount_trips"),
            sum_flag("is_missing_fare").alias("missing_fare_trips"),
            sum_flag("is_missing_total").alias("missing_total_trips"),
            sum_flag("is_missing_amount").alias("missing_amount_trips"),
            sum_flag("is_passenger_missing").alias("missing_passenger_trips"),
            sum_flag("is_missing_pickup_location").alias("missing_pu_location_trips"),
            sum_flag("is_duplicate_record").alias("duplicate_record_trips"),
            # TLC payment type counts and Flex Fare slice
            F.count(F.when(is_flex, True)).alias("flex_fare_trip_count"),
            F.count(F.when(F.col("payment_type") == 1, True)).alias("credit_card_trip_count"),
            F.count(F.when(F.col("payment_type") == 2, True)).alias("cash_trip_count"),
            F.count(F.when(F.col("payment_type") == 3, True)).alias("no_charge_trip_count"),
            F.count(F.when(F.col("payment_type") == 4, True)).alias("dispute_trip_count"),
            F.count(F.when(F.col("payment_type") == 5, True)).alias("unknown_payment_trip_count"),
            F.count(F.when(F.col("payment_type") == 6, True)).alias("voided_trip_count"),
            F.count(
                F.when(
                    F.col("payment_type").isNull()
                    | (~F.col("payment_type").isin(0, 1, 2, 3, 4, 5, 6)),
                    True,
                )
            ).alias("other_payment_trip_count"),
            sum_if(is_flex & (~F.col("is_missing_fare")), "fare_amount").alias("net_fare_flex_usd"),
            sum_if(is_flex & (~F.col("is_missing_total")), "total_amount").alias("net_total_amount_flex_usd"),
        )
    )

    fact = date_grid.join(daily_agg, on="date_key", how="left")
    for col_name in COUNT_ZERO_FILL_COLS:
        fact = fact.withColumn(col_name, F.coalesce(F.col(col_name), F.lit(0)).cast("long"))
    for col_name in AMOUNT_ZERO_FILL_COLS:
        fact = fact.withColumn(col_name, F.coalesce(F.col(col_name), F.lit(0.0)).cast("double"))

    fact = (
        fact
        .withColumn("has_observed_trips", F.col("trip_count") > 0)
        .withColumn(
            "zero_distance_pct",
            F.when(
                F.col("trip_count") > 0,
                F.round(F.col("zero_distance_trips") * 100.0 / F.col("trip_count"), 2),
            ),
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

    print(f"{year_month}: done ({len(month_dates)} calendar days)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Validation
# Reconciles the fact table against Silver for every month in `MONTHS`; fails the notebook on any mismatch.

# CELL ********************

from pyspark.sql import functions as F

fact   = spark.table("dbo.fact_taxi_daily")
dim    = spark.table("dbo.dim_date")
silver = spark.table("lh_silver.dbo.silver_taxi")
audit  = spark.table("lh_silver.dbo.silver_taxi_quality_audit")
problems = []

is_nonnegative_fare = (~F.col("is_missing_fare")) & (~F.col("is_negative_fare"))
is_nonnegative_total = (~F.col("is_missing_total")) & (~F.col("is_negative_total"))
is_nonnegative_revenue = (~F.col("is_missing_amount")) & (~F.col("is_negative_amount"))

# ---- 1. per-month reconciliation: fact vs silver (scoped to MONTHS) ----
fact_m = (
    fact.join(dim, "date_key")
    .filter(F.col("year_month").isin(MONTHS))
    .groupBy("year_month")
    .agg(
        F.count("*").alias("fact_days"),
        F.sum("trip_count").alias("fact_trips"),
        F.sum("revenue_trip_count").alias("fact_rtc"),
        F.sum("nonnegative_fare_trip_count").alias("fact_nn_fare_tc"),
        F.sum("nonnegative_total_trip_count").alias("fact_nn_total_tc"),
        F.sum("flex_fare_trip_count").alias("fact_pt0"),
        F.sum("zero_distance_trips").alias("fact_zero_dist"),
        F.sum("negative_distance_trips").alias("fact_neg_dist"),
        F.sum("negative_amount_trips").alias("fact_neg_amt"),
        F.sum("missing_amount_trips").alias("fact_miss_amt"),
        F.round(F.sum("total_fare_usd"), 2).alias("fact_gross_rev_fare"),
        F.round(F.sum("total_amount_usd"), 2).alias("fact_gross_rev_total"),
        F.round(F.sum("net_total_amount_usd"), 2).alias("fact_net_total"),
    )
)

silver_m = (
    silver.filter(F.col("year_month").isin(MONTHS))
    .groupBy("year_month")
    .agg(
        F.count("*").alias("silver_trips"),
        F.sum(is_nonnegative_revenue.cast("long")).alias("silver_rtc"),
        F.sum(is_nonnegative_fare.cast("long")).alias("silver_nn_fare_tc"),
        F.sum(is_nonnegative_total.cast("long")).alias("silver_nn_total_tc"),
        F.sum((F.col("payment_type") == 0).cast("long")).alias("silver_pt0"),
        F.sum(F.col("is_zero_distance").cast("long")).alias("silver_zero_dist"),
        F.sum(F.col("is_negative_distance").cast("long")).alias("silver_neg_dist"),
        F.sum(F.col("is_negative_amount").cast("long")).alias("silver_neg_amt"),
        F.sum(F.col("is_missing_amount").cast("long")).alias("silver_miss_amt"),
        F.round(F.coalesce(F.sum(F.when(is_nonnegative_revenue, F.col("fare_amount"))), F.lit(0.0)), 2).alias("silver_gross_rev_fare"),
        F.round(F.coalesce(F.sum(F.when(is_nonnegative_revenue, F.col("total_amount"))), F.lit(0.0)), 2).alias("silver_gross_rev_total"),
        F.round(F.coalesce(F.sum(F.when(~F.col("is_missing_total"), F.col("total_amount"))), F.lit(0.0)), 2).alias("silver_net_total"),
    )
)

dim_m = dim.filter(F.col("year_month").isin(MONTHS)).groupBy("year_month").agg(F.count("*").alias("calendar_days"))

recon = (
    silver_m.join(fact_m, "year_month", "full_outer")
    .join(dim_m, "year_month", "left")
    .withColumn("trips_diff", F.col("silver_trips") - F.col("fact_trips"))
    .withColumn("rtc_diff", F.col("silver_rtc") - F.col("fact_rtc"))
    .withColumn("nn_fare_tc_diff", F.col("silver_nn_fare_tc") - F.col("fact_nn_fare_tc"))
    .withColumn("nn_total_tc_diff", F.col("silver_nn_total_tc") - F.col("fact_nn_total_tc"))
    .withColumn("pt0_diff", F.col("silver_pt0") - F.col("fact_pt0"))
    .withColumn("zero_dist_diff", F.col("silver_zero_dist") - F.col("fact_zero_dist"))
    .withColumn("neg_dist_diff", F.col("silver_neg_dist") - F.col("fact_neg_dist"))
    .withColumn("neg_amt_diff", F.col("silver_neg_amt") - F.col("fact_neg_amt"))
    .withColumn("miss_amt_diff", F.col("silver_miss_amt") - F.col("fact_miss_amt"))
    .withColumn("gross_rev_fare_diff", F.round(F.col("silver_gross_rev_fare") - F.col("fact_gross_rev_fare"), 2))
    .withColumn("gross_rev_total_diff", F.round(F.col("silver_gross_rev_total") - F.col("fact_gross_rev_total"), 2))
    .withColumn("net_total_diff", F.round(F.col("silver_net_total") - F.col("fact_net_total"), 2))
    .withColumn("days_diff", F.col("calendar_days") - F.col("fact_days"))
    .orderBy("year_month")
)

recon.select(
    "year_month", "silver_trips", "fact_trips", "trips_diff",
    "rtc_diff", "pt0_diff", "zero_dist_diff", "neg_amt_diff",
    "miss_amt_diff", "net_total_diff", "fact_days", "calendar_days", "days_diff"
).show(truncate=False)

bad_months = recon.filter(
    F.col("trips_diff").isNull() | (F.col("trips_diff") != 0)
    | F.col("rtc_diff").isNull() | (F.col("rtc_diff") != 0)
    | F.col("nn_fare_tc_diff").isNull() | (F.col("nn_fare_tc_diff") != 0)
    | F.col("nn_total_tc_diff").isNull() | (F.col("nn_total_tc_diff") != 0)
    | F.col("pt0_diff").isNull() | (F.col("pt0_diff") != 0)
    | F.col("zero_dist_diff").isNull() | (F.col("zero_dist_diff") != 0)
    | F.col("neg_dist_diff").isNull() | (F.col("neg_dist_diff") != 0)
    | F.col("neg_amt_diff").isNull() | (F.col("neg_amt_diff") != 0)
    | F.col("miss_amt_diff").isNull() | (F.col("miss_amt_diff") != 0)
    | F.col("gross_rev_fare_diff").isNull() | (F.abs(F.col("gross_rev_fare_diff")) > 1.0)
    | F.col("gross_rev_total_diff").isNull() | (F.abs(F.col("gross_rev_total_diff")) > 1.0)
    | F.col("net_total_diff").isNull() | (F.abs(F.col("net_total_diff")) > 1.0)
    | F.col("days_diff").isNull() | (F.col("days_diff") != 0)
).count()
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

must_have = [
    "trip_count", "revenue_trip_count", "total_fare_usd", "total_amount_usd",
    "gross_nonnegative_fare_usd", "gross_nonnegative_total_amount_usd",
    "net_fare_usd", "net_total_amount_usd", "zero_distance_trips",
    "negative_amount_trips", "missing_amount_trips",
    "flex_fare_trip_count", "net_fare_flex_usd", "net_total_amount_flex_usd",
    "has_observed_trips",
]
null_rows = (
    fact.join(dim.filter(F.col("year_month").isin(MONTHS)).select("date_key"), "date_key")
    .filter(F.expr(" OR ".join(f"{c} IS NULL" for c in must_have)))
    .count()
)
print("rows in MONTHS with null in required columns:", null_rows)
if null_rows:
    problems.append(f"{null_rows} fact rows have nulls in required columns")

print("dim_date range:", dim.agg(F.min("full_date"), F.max("full_date")).first())

# ---- 3. MONTHS coverage & Silver audit ----
silver_months = {r[0] for r in silver.select("year_month").distinct().collect()}
fact_months   = {r[0] for r in fact.join(dim, "date_key").select("year_month").distinct().collect()}
passed_audit_months = {
    r[0] for r in audit.filter(F.col("reconciliation_passed")).select("source_year_month").collect()
}
miss_silver = sorted(set(MONTHS) - silver_months)
miss_fact   = sorted(set(MONTHS) - fact_months)
miss_audit  = sorted(set(MONTHS) - passed_audit_months)
print("MONTHS missing from silver:", miss_silver)
print("MONTHS missing from fact:  ", miss_fact)
print("MONTHS missing passed audit:", miss_audit)
if miss_silver: problems.append(f"missing from silver: {miss_silver}")
if miss_fact:   problems.append(f"missing from fact: {miss_fact}")
if miss_audit:  problems.append(f"missing passed audit: {miss_audit}")

if problems:
    msg = "PROBLEMS: " + "; ".join(problems)
    print("\n" + msg)
    raise ValueError(msg)

print("\nALL CHECKS PASSED")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
