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
# The pipeline passes the resolved month list as JSON. Set `MONTHS_JSON` when running interactively.

# PARAMETERS CELL ********************

MONTHS_JSON = "[]"

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

# MARKDOWN ********************

# ## Reference Dimensions and Annual GDP

# CELL ********************

from datetime import date, timedelta
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DateType, StringType, StructField, StructType

AUDIT_TABLE = "lh_silver.dbo.silver_economics_quality_audit"
SILVER_GDP_TABLE = "lh_silver.dbo.silver_world_bank_gdp"
SILVER_GDP_Q_TABLE = "lh_silver.dbo.silver_world_bank_gdp_quarantine"
SILVER_FX_TABLE = "lh_silver.dbo.silver_ecb_fx"

if not spark.catalog.tableExists(AUDIT_TABLE):
    raise ValueError(f"Missing Silver economics audit table {AUDIT_TABLE}; refusing to publish Gold")

audit_rows = {r["dataset"]: r for r in spark.table(AUDIT_TABLE).collect()}
for ds in ("WORLD_BANK_GDP", "ECB_FX"):
    if ds not in audit_rows or not audit_rows[ds]["reconciliation_passed"]:
        raise ValueError(f"Silver economics audit check failed or missing for {ds}")

CURRENCY_SCHEMA = StructType([
    StructField("currency_code", StringType(), False),
    StructField("currency_name", StringType(), False),
    StructField("symbol", StringType(), False),
    StructField("issuing_authority", StringType(), False),
    StructField("is_reference_currency", BooleanType(), False),
])
currency_rows = [
    ("USD", "United States Dollar", "$", "Federal Reserve", False),
    ("EUR", "Euro", "€", "European Central Bank", True),
]
dim_currency = spark.createDataFrame(currency_rows, schema=CURRENCY_SCHEMA)
(
    dim_currency.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("dbo.dim_currency")
)

# Include quarantined entities so country metadata remains complete.
silver_gdp = spark.table(SILVER_GDP_TABLE)
silver_gdp_q = spark.table(SILVER_GDP_Q_TABLE)
all_gdp_entities = silver_gdp.select(
    "country_iso3", "country_code_2", "country_name"
).unionByName(
    silver_gdp_q.select("country_iso3", "country_code_2", "country_name")
)

dim_country = (
    all_gdp_entities
    .filter(F.col("country_iso3").isNotNull())
    .dropDuplicates(["country_iso3"])
    .withColumn(
        "primary_currency_code",
        F.when(F.col("country_iso3") == "USA", F.lit("USD"))
        .when(F.col("country_iso3") == "EMU", F.lit("EUR"))
        .otherwise(F.lit(None).cast("string")),
    )
    .withColumn(
        "entity_type",
        F.when(F.col("country_iso3") == "EMU", F.lit("Monetary Union Aggregate"))
        .otherwise(F.lit("Country")),
    )
    .orderBy("country_iso3")
)
(
    dim_country.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("dbo.dim_country")
)

fact_gdp_annual = (
    silver_gdp
    .select(
        "country_iso3",
        "country_name",
        "year",
        "indicator_id",
        "indicator_name",
        "gdp_usd",
        "last_updated_date",
        "ingested_at_utc",
    )
    .orderBy("country_iso3", "year")
)
(
    fact_gdp_annual.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("dbo.fact_gdp_annual")
)

print(f"wrote dim_currency ({dim_currency.count()}), dim_country ({dim_country.count()}), fact_gdp_annual ({fact_gdp_annual.count()})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Daily FX with Carry-Forward Policy

# CELL ********************

def next_month(ms):
    y, m = int(ms[:4]), int(ms[5:7])
    return f"{y + (m == 12)}-{(m % 12) + 1:02d}-01"


silver_fx = spark.table(SILVER_FX_TABLE)
fx_bounds = silver_fx.agg(
    F.min("observation_date").alias("min_fx_date"),
    F.max("observation_date").alias("max_fx_date"),
).first()
if fx_bounds["min_fx_date"] is None:
    raise ValueError("No valid ECB FX observations found in Silver.")

selected_min = min(date.fromisoformat(f"{ym}-01") for ym in MONTHS)
selected_max = max(date.fromisoformat(next_month(f"{ym}-01")) - timedelta(days=1) for ym in MONTHS)
spine_start = min(fx_bounds["min_fx_date"], selected_min)
spine_end = max(fx_bounds["max_fx_date"], selected_max)

spine_dates = [
    (spine_start + timedelta(days=i),)
    for i in range((spine_end - spine_start).days + 1)
]
date_spine = spark.createDataFrame(
    spine_dates,
    StructType([StructField("full_date", DateType(), False)]),
)

pairs_df = silver_fx.select("currency_pair", "base_currency", "quote_currency", "quote_direction").distinct()

carry_window = (
    Window.partitionBy("currency_pair")
    .orderBy("full_date")
    .rowsBetween(Window.unboundedPreceding, 0)
)

fx_daily_full = (
    date_spine.crossJoin(pairs_df).alias("g")
    .join(
        silver_fx.select(
            F.col("currency_pair").alias("fx_pair"),
            F.col("observation_date").alias("fx_obs_date"),
            "usd_per_eur_rate",
            "eur_per_usd_rate",
            "obs_status",
            "unit",
        ).alias("fx"),
        (F.col("g.currency_pair") == F.col("fx.fx_pair"))
        & (F.col("g.full_date") == F.col("fx.fx_obs_date")),
        how="left",
    )
    .select(
        F.col("g.full_date"),
        F.col("g.currency_pair"),
        F.col("g.base_currency"),
        F.col("g.quote_currency"),
        F.col("g.quote_direction"),
        F.last("fx.usd_per_eur_rate", ignorenulls=True).over(carry_window).alias("usd_per_eur_rate"),
        F.last("fx.eur_per_usd_rate", ignorenulls=True).over(carry_window).alias("eur_per_usd_rate"),
        F.last("fx.fx_obs_date", ignorenulls=True).over(carry_window).alias("rate_observation_date"),
        F.last("fx.obs_status", ignorenulls=True).over(carry_window).alias("obs_status"),
        F.last("fx.unit", ignorenulls=True).over(carry_window).alias("unit"),
    )
    .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
    .withColumn("is_carry_forward_rate", F.col("rate_observation_date") < F.col("full_date"))
    .withColumn("days_since_rate_observation", F.datediff("full_date", "rate_observation_date"))
    .withColumn("rate_policy", F.lit("ECB_LAST_BUSINESS_DAY_CARRY_FORWARD"))
)

FX_GOLD_TABLE = "dbo.fact_fx_daily"
for ym in sorted(MONTHS):
    month_start = f"{ym}-01"
    month_end = next_month(month_start)
    lo = int(month_start.replace("-", ""))
    hi = int(month_end.replace("-", ""))

    month_fx = fx_daily_full.filter((F.col("date_key") >= lo) & (F.col("date_key") < hi))
    if spark.catalog.tableExists(FX_GOLD_TABLE):
        (
            month_fx.write.format("delta").mode("overwrite")
            .option("replaceWhere", f"date_key >= {lo} AND date_key < {hi}")
            .option("mergeSchema", "true")
            .saveAsTable(FX_GOLD_TABLE)
        )
    else:
        month_fx.write.format("delta").saveAsTable(FX_GOLD_TABLE)
    print(f"fact_fx_daily [{ym}]: saved")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Validation

# CELL ********************

dim_ccy = spark.table("dbo.dim_currency")
dim_ctry = spark.table("dbo.dim_country")
fact_gdp = spark.table("dbo.fact_gdp_annual")
fact_fx = spark.table("dbo.fact_fx_daily")

problems = []

if dim_ccy.count() < 2 or dim_ccy.filter(F.col("currency_code").isNull()).count() > 0:
    problems.append("dim_currency invalid")
if dim_ctry.count() == 0 or (dim_ctry.count() - dim_ctry.select("country_iso3").distinct().count()) > 0:
    problems.append("dim_country has missing or duplicate country_iso3")

gdp_dups = fact_gdp.groupBy("country_iso3", "year").count().filter(F.col("count") > 1).count()
if gdp_dups > 0:
    problems.append(f"fact_gdp_annual has {gdp_dups} duplicate (country_iso3, year) rows")
if fact_gdp.count() != spark.table(SILVER_GDP_TABLE).count():
    problems.append("fact_gdp_annual count does not match silver_world_bank_gdp")

expected_fx_days = sum(
    (date.fromisoformat(next_month(f"{ym}-01")) - date.fromisoformat(f"{ym}-01")).days
    for ym in MONTHS
)
num_pairs = pairs_df.count()
expected_fx_rows = expected_fx_days * num_pairs

date_filters = None
for ym in MONTHS:
    m_start = f"{ym}-01"
    m_end = next_month(m_start)
    lo = int(m_start.replace("-", ""))
    hi = int(m_end.replace("-", ""))
    cond = (F.col("date_key") >= lo) & (F.col("date_key") < hi)
    date_filters = cond if date_filters is None else (date_filters | cond)

fx_scoped = fact_fx.filter(date_filters)
actual_fx_rows = fx_scoped.count()
fx_dups = fx_scoped.groupBy("date_key", "currency_pair").count().filter(F.col("count") > 1).count()
fx_nulls = fx_scoped.filter(
    F.col("usd_per_eur_rate").isNull()
    | F.col("eur_per_usd_rate").isNull()
    | F.col("rate_observation_date").isNull()
    | (F.col("rate_observation_date") > F.col("full_date"))
).count()

if actual_fx_rows != expected_fx_rows:
    problems.append(f"fact_fx_daily row mismatch for MONTHS: expected {expected_fx_rows}, got {actual_fx_rows}")
if fx_dups > 0:
    problems.append(f"fact_fx_daily has {fx_dups} duplicate (date_key, currency_pair) rows")
if fx_nulls > 0:
    problems.append(f"fact_fx_daily has {fx_nulls} rows with null rates or invalid observation dates")

if spark.catalog.tableExists("dbo.dim_date"):
    missing_from_dim = fx_scoped.join(spark.table("dbo.dim_date"), "date_key", "left_anti").count()
    if missing_from_dim > 0:
        problems.append(f"fact_fx_daily has {missing_from_dim} date_keys missing from dim_date")

print(f"fact_gdp_annual rows: {fact_gdp.count():,}")
print(f"fact_fx_daily rows in MONTHS: {actual_fx_rows:,} (expected={expected_fx_rows:,})")
fx_scoped.agg(
    F.count("*").alias("total_rows"),
    F.sum((~F.col("is_carry_forward_rate")).cast("long")).alias("published_rate_days"),
    F.sum(F.col("is_carry_forward_rate").cast("long")).alias("carry_forward_days"),
    F.max("days_since_rate_observation").alias("max_carry_forward_gap_days"),
).show(truncate=False)

if problems:
    raise ValueError("PROBLEMS: " + "; ".join(problems))

print("ALL CHECKS PASSED")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
