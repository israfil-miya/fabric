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
# Validates the shared `MONTHS_JSON` parameter contract before transforming Bronze GDP and ECB FX tables.

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
print("months context:", MONTHS)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 1: Clean, classify, and reconcile World Bank annual GDP

from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType, TimestampType

BRONZE_GDP_TABLE = "lh_bronze.dbo.bronze_world_bank_gdp"
SILVER_GDP_TABLE = "dbo.silver_world_bank_gdp"
SILVER_GDP_QUARANTINE = "dbo.silver_world_bank_gdp_quarantine"

bronze_gdp = spark.table(BRONZE_GDP_TABLE)
gdp_dups = (
    bronze_gdp.groupBy("indicator_id", "country_iso3", "year_raw")
    .count()
    .filter(F.col("count") > 1)
    .count()
)
if gdp_dups > 0:
    raise ValueError(f"Found {gdp_dups} duplicate World Bank GDP keys in Bronze.")

is_missing_gdp = F.col("value").isNull() | F.isnan(F.col("value"))
gdp_classified = (
    bronze_gdp
    .withColumn(
        "quality_status",
        F.when(
            F.col("country_iso3").isNull()
            | (F.trim(F.col("country_iso3")) == "")
            | F.col("year").isNull(),
            F.lit("INVALID_KEY"),
        )
        .when(is_missing_gdp, F.lit("MISSING_VALUE"))
        .when(F.col("value") <= 0, F.lit("NON_POSITIVE_VALUE"))
        .otherwise(F.lit("VALID")),
    )
    .withColumn("gdp_usd", F.col("value").cast("double"))
    .cache()
)

silver_gdp = gdp_classified.filter(F.col("quality_status") == "VALID").drop("value")
quarantine_gdp = gdp_classified.filter(F.col("quality_status") != "VALID").drop("value")

(
    silver_gdp.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_GDP_TABLE)
)
(
    quarantine_gdp.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_GDP_QUARANTINE)
)

bronze_gdp_n = gdp_classified.count()
written_silver_gdp_n = spark.table(SILVER_GDP_TABLE).count()
written_quarantine_gdp_n = spark.table(SILVER_GDP_QUARANTINE).count()
gdp_diff = bronze_gdp_n - (written_silver_gdp_n + written_quarantine_gdp_n)
gdp_passed = (gdp_diff == 0) and (written_silver_gdp_n > 0)

gdp_classified.unpersist()

print(
    f"World Bank GDP: bronze={bronze_gdp_n:,}, valid={written_silver_gdp_n:,}, "
    f"quarantined={written_quarantine_gdp_n:,}, diff={gdp_diff}"
)
if not gdp_passed:
    raise ValueError(
        f"World Bank GDP Silver reconciliation failed: bronze={bronze_gdp_n}, "
        f"silver={written_silver_gdp_n}, quarantine={written_quarantine_gdp_n}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 2: Clean, classify, and reconcile ECB daily FX rates

BRONZE_FX_TABLE = "lh_bronze.dbo.bronze_ecb_fx"
SILVER_FX_TABLE = "dbo.silver_ecb_fx"
SILVER_FX_QUARANTINE = "dbo.silver_ecb_fx_quarantine"
AUDIT_TABLE = "dbo.silver_economics_quality_audit"

bronze_fx = spark.table(BRONZE_FX_TABLE)
fx_dups = (
    bronze_fx.groupBy("currency_pair", "observation_date_str")
    .count()
    .filter(F.col("count") > 1)
    .count()
)
if fx_dups > 0:
    raise ValueError(f"Found {fx_dups} duplicate ECB FX keys in Bronze.")

is_missing_fx = F.col("exchange_rate_value").isNull() | F.isnan(F.col("exchange_rate_value"))
fx_classified = (
    bronze_fx
    .withColumn("observation_date", F.to_date(F.col("observation_date_str"), "yyyy-MM-dd"))
    .withColumn(
        "quality_status",
        F.when(F.col("observation_date").isNull(), F.lit("INVALID_DATE"))
        .when(is_missing_fx, F.lit("MISSING_RATE"))
        .when(F.col("exchange_rate_value") <= 0, F.lit("NON_POSITIVE_RATE"))
        .otherwise(F.lit("VALID")),
    )
    .withColumn("usd_per_eur_rate", F.col("exchange_rate_value").cast("double"))
    .withColumn(
        "eur_per_usd_rate",
        F.when(F.col("quality_status") == "VALID", F.lit(1.0) / F.col("exchange_rate_value").cast("double")),
    )
    .cache()
)

silver_fx = fx_classified.filter(F.col("quality_status") == "VALID")
quarantine_fx = fx_classified.filter(F.col("quality_status") != "VALID")

(
    silver_fx.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_FX_TABLE)
)
(
    quarantine_fx.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_FX_QUARANTINE)
)

bronze_fx_n = fx_classified.count()
written_silver_fx_n = spark.table(SILVER_FX_TABLE).count()
written_quarantine_fx_n = spark.table(SILVER_FX_QUARANTINE).count()
fx_diff = bronze_fx_n - (written_silver_fx_n + written_quarantine_fx_n)
fx_passed = (fx_diff == 0) and (written_silver_fx_n > 0)

fx_classified.unpersist()

print(
    f"ECB FX: bronze={bronze_fx_n:,}, valid={written_silver_fx_n:,}, "
    f"quarantined={written_quarantine_fx_n:,}, diff={fx_diff}"
)
if not fx_passed:
    raise ValueError(
        f"ECB FX Silver reconciliation failed: bronze={bronze_fx_n}, "
        f"silver={written_silver_fx_n}, quarantine={written_quarantine_fx_n}"
    )

audit_ts = datetime.now(timezone.utc).replace(tzinfo=None)
AUDIT_SCHEMA = StructType([
    StructField("dataset", StringType(), False),
    StructField("bronze_row_count", LongType(), False),
    StructField("silver_row_count", LongType(), False),
    StructField("quarantine_row_count", LongType(), False),
    StructField("reconciliation_diff", LongType(), False),
    StructField("reconciliation_passed", BooleanType(), False),
    StructField("audit_timestamp_utc", TimestampType(), False),
])
audit_rows = [
    ("WORLD_BANK_GDP", int(bronze_gdp_n), int(written_silver_gdp_n), int(written_quarantine_gdp_n), int(gdp_diff), bool(gdp_passed), audit_ts),
    ("ECB_FX", int(bronze_fx_n), int(written_silver_fx_n), int(written_quarantine_fx_n), int(fx_diff), bool(fx_passed), audit_ts),
]
(
    spark.createDataFrame(audit_rows, schema=AUDIT_SCHEMA)
    .write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(AUDIT_TABLE)
)

spark.table(AUDIT_TABLE).show(truncate=False)
print("ALL CHECKS PASSED")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
