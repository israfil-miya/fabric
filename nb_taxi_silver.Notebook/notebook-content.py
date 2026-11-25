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

# PARAMETERS CELL ********************

MONTHS_JSON = "[]"
INSPECT_SAMPLES = False

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

# MARKDOWN ********************
# ## Helpers and Quality Rules


# CELL ********************

from datetime import datetime, timezone
from pyspark.sql import Window
from pyspark.sql import functions as F

target = "dbo.silver_taxi"
quarantine_target = "dbo.silver_taxi_quarantine"
audit_target = "dbo.silver_taxi_quality_audit"

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

REQUIRED = {
    "tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance",
    "fare_amount", "total_amount", "PULocationID",
}

QUALITY_FLAG_LABELS = [
    ("is_zero_distance", "ZERO_DISTANCE"),
    ("is_missing_distance", "MISSING_DISTANCE"),
    ("is_negative_distance", "NEGATIVE_DISTANCE"),
    ("is_unusual_distance", "UNUSUAL_DISTANCE_GT_100"),
    ("is_missing_dropoff_datetime", "MISSING_DROPOFF"),
    ("is_zero_duration", "ZERO_DURATION"),
    ("is_unusual_duration", "UNUSUAL_DURATION_GT_480"),
    ("is_negative_fare", "NEGATIVE_FARE"),
    ("is_negative_total", "NEGATIVE_TOTAL"),
    ("is_missing_fare", "MISSING_FARE"),
    ("is_missing_total", "MISSING_TOTAL"),
    ("is_passenger_missing", "MISSING_PASSENGER"),
    ("is_missing_pickup_location", "MISSING_PU_LOCATION"),
    ("is_duplicate_record", "DUPLICATE_RECORD"),
]

QUARANTINE_FLAG_LABELS = [
    ("is_missing_pickup_datetime", "MISSING_PICKUP_DATETIME"),
    ("is_dropoff_before_pickup", "DROPOFF_BEFORE_PICKUP"),
    ("is_out_of_month", "OUT_OF_MONTH_PICKUP"),
]


def next_month(ms):
    y, m = int(ms[:4]), int(ms[5:7])
    return f"{y + (m == 12)}-{(m % 12) + 1:02d}-01"


def is_missing_num(col_name):
    c = F.col(col_name)
    return c.isNull() | F.isnan(c)


def write_month_table(df, table_name, partition_col, year_month):
    if spark.catalog.tableExists(table_name):
        (
            df.write.mode("overwrite").format("delta")
            .option("replaceWhere", f"{partition_col} = '{year_month}'")
            .option("mergeSchema", "true")
            .saveAsTable(table_name)
        )
    else:
        df.write.format("delta").saveAsTable(table_name)


def load_month(month_start):
    year_month = month_start[:7]
    month_end = next_month(month_start)
    source_file = f"yellow_tripdata_{year_month}.parquet"
    raw_in = spark.read.parquet(f"{BRONZE_TAXI_PATH}/{source_file}")
    in_type = dict(raw_in.dtypes).get("tpep_pickup_datetime", "missing")

    lower = {c.lower(): c for c in raw_in.columns}
    missing_req = {c for c in REQUIRED if c.lower() not in lower}
    assert not missing_req, f"missing required columns: {missing_req}"

    raw = raw_in.select(*[
        (F.col(lower[n.lower()]).cast(t) if n.lower() in lower else F.lit(None).cast(t)).alias(n)
        for n, t in SCHEMA
    ])

    month_start_ts = F.lit(month_start).cast("timestamp_ntz")
    month_end_ts = F.lit(month_end).cast("timestamp_ntz")

    # Hash full source row for non-destructive duplicate detection.
    record_hash_expr = F.sha2(F.to_json(F.struct(*[F.col(n) for n, _ in SCHEMA])), 256)
    dup_window = Window.partitionBy("record_hash")

    classified = (
        raw
        .withColumn("source_year_month", F.lit(year_month))
        .withColumn("source_file", F.lit(source_file))
        .withColumn("record_hash", record_hash_expr)
        .withColumn("duplicate_group_size", F.count("*").over(dup_window))
        .withColumn(
            "pickup_date",
            F.when(F.col("tpep_pickup_datetime").isNotNull(), F.to_date("tpep_pickup_datetime")),
        )
        .withColumn(
            "pickup_hour",
            F.when(F.col("tpep_pickup_datetime").isNotNull(), F.hour("tpep_pickup_datetime")),
        )
        .withColumn(
            "trip_duration_min",
            F.when(
                F.col("tpep_pickup_datetime").isNotNull() & F.col("tpep_dropoff_datetime").isNotNull(),
                (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60.0,
            ),
        )
        .withColumn("is_missing_pickup_datetime", F.col("tpep_pickup_datetime").isNull())
        .withColumn("is_missing_dropoff_datetime", F.col("tpep_dropoff_datetime").isNull())
        .withColumn(
            "is_dropoff_before_pickup",
            F.col("tpep_pickup_datetime").isNotNull()
            & F.col("tpep_dropoff_datetime").isNotNull()
            & (F.col("tpep_dropoff_datetime") < F.col("tpep_pickup_datetime")),
        )
        .withColumn(
            "is_out_of_month",
            F.col("tpep_pickup_datetime").isNotNull()
            & ((F.col("tpep_pickup_datetime") < month_start_ts) | (F.col("tpep_pickup_datetime") >= month_end_ts)),
        )
        .withColumn("is_missing_distance", is_missing_num("trip_distance"))
        .withColumn("is_zero_distance", (~is_missing_num("trip_distance")) & (F.col("trip_distance") == 0))
        .withColumn("is_negative_distance", (~is_missing_num("trip_distance")) & (F.col("trip_distance") < 0))
        .withColumn("is_unusual_distance", (~is_missing_num("trip_distance")) & (F.col("trip_distance") > 100))
        .withColumn(
            "is_zero_duration",
            F.col("tpep_pickup_datetime").isNotNull()
            & F.col("tpep_dropoff_datetime").isNotNull()
            & (F.col("tpep_dropoff_datetime") == F.col("tpep_pickup_datetime")),
        )
        .withColumn("is_unusual_duration", F.col("trip_duration_min").isNotNull() & (F.col("trip_duration_min") > 480))
        .withColumn("is_missing_fare", is_missing_num("fare_amount"))
        .withColumn("is_missing_total", is_missing_num("total_amount"))
        .withColumn("is_missing_amount", is_missing_num("fare_amount") | is_missing_num("total_amount"))
        .withColumn("is_negative_fare", (~is_missing_num("fare_amount")) & (F.col("fare_amount") < 0))
        .withColumn("is_negative_total", (~is_missing_num("total_amount")) & (F.col("total_amount") < 0))
        .withColumn(
            "is_negative_amount",
            ((~is_missing_num("fare_amount")) & (F.col("fare_amount") < 0))
            | ((~is_missing_num("total_amount")) & (F.col("total_amount") < 0)),
        )
        .withColumn("is_passenger_missing", F.col("passenger_count").isNull())
        .withColumn("is_missing_pickup_location", F.col("PULocationID").isNull())
        .withColumn("is_duplicate_record", F.col("duplicate_group_size") > 1)
    )

    flag_tokens = [F.when(F.col(col_name), F.lit(label)) for col_name, label in QUALITY_FLAG_LABELS]
    quarantine_tokens = [F.when(F.col(col_name), F.lit(label)) for col_name, label in QUARANTINE_FLAG_LABELS]
    issue_count_expr = sum(F.col(col_name).cast("int") for col_name, _ in QUALITY_FLAG_LABELS)

    classified = (
        classified
        .withColumn("quality_issue_count", issue_count_expr)
        .withColumn("has_any_quality_flag", F.col("quality_issue_count") > 0)
        .withColumn("quality_flags", F.concat_ws(",", *flag_tokens))
        .withColumn("quarantine_reasons", F.concat_ws(",", *quarantine_tokens))
        .withColumn(
            "is_quarantined",
            F.col("is_missing_pickup_datetime") | F.col("is_dropoff_before_pickup") | F.col("is_out_of_month"),
        )
        .cache()
    )

    try:
        raw_n = classified.count()
        if raw_n == 0:
            raise ValueError(f"source Parquet file for {year_month} is empty")

        silver = (
            classified
            .filter(~F.col("is_quarantined"))
            .withColumn("year_month", F.lit(year_month))
            .drop("is_quarantined", "quarantine_reasons",
                  "is_missing_pickup_datetime", "is_dropoff_before_pickup", "is_out_of_month")
        )
        quarantine = (
            classified
            .filter(F.col("is_quarantined"))
            .withColumn(
                "quarantine_scope",
                F.when(F.col("is_out_of_month"), F.lit("OUT_OF_MONTH"))
                .otherwise(F.lit("IN_SCOPE_QUARANTINE")),
            )
            .drop("is_quarantined")
        )

        bad = silver.filter(
            F.col("pickup_date").isNull()
            | (F.col("pickup_date") < month_start)
            | (F.col("pickup_date") >= month_end)
        ).count()
        assert bad == 0, f"{bad} rows in silver_taxi fall outside {year_month}"

        silver_n = silver.count()
        quarantine_n = quarantine.count()
        if silver_n == 0:
            raise ValueError(f"No in-scope rows remained for {year_month}")
        if silver_n + quarantine_n != raw_n:
            raise ValueError(
                f"Pre-write reconciliation failed for {year_month}: "
                f"raw={raw_n}, silver={silver_n}, quarantine={quarantine_n}"
            )

        # Track what the pre-milestone filter would have discarded silently.
        legacy_kept_cond = (
            (~F.col("is_out_of_month"))
            & (~F.col("is_missing_pickup_datetime"))
            & (~F.col("is_missing_dropoff_datetime"))
            & (~F.col("is_dropoff_before_pickup"))
            & (~F.col("is_missing_distance"))
            & (F.col("trip_distance") <= 100)
            & (F.col("trip_duration_min") <= 480)
        )

        audit_stats = classified.agg(
            F.count("*").alias("bronze_row_count"),
            F.sum((~F.col("is_quarantined")).cast("long")).alias("silver_row_count"),
            F.sum(F.col("is_quarantined").cast("long")).alias("quarantine_total_count"),
            F.sum((F.col("is_quarantined") & (~F.col("is_out_of_month"))).cast("long")).alias("quarantine_in_scope_count"),
            F.sum(F.col("is_out_of_month").cast("long")).alias("quarantine_out_of_month_count"),
            F.sum(F.col("is_missing_pickup_datetime").cast("long")).alias("quarantine_missing_pickup_count"),
            F.sum(F.col("is_dropoff_before_pickup").cast("long")).alias("quarantine_dropoff_before_pickup_count"),
            F.sum((~F.col("is_quarantined") & F.col("is_zero_distance")).cast("long")).alias("silver_zero_distance_count"),
            F.sum((~F.col("is_quarantined") & F.col("is_missing_distance")).cast("long")).alias("silver_missing_distance_count"),
            F.sum((~F.col("is_quarantined") & F.col("is_negative_distance")).cast("long")).alias("silver_negative_distance_count"),
            F.sum((~F.col("is_quarantined") & F.col("is_unusual_distance")).cast("long")).alias("silver_unusual_distance_gt_100_count"),
            F.sum((~F.col("is_quarantined") & F.col("is_missing_dropoff_datetime")).cast("long")).alias("silver_missing_dropoff_count"),
            F.sum((~F.col("is_quarantined") & F.col("is_zero_duration")).cast("long")).alias("silver_zero_duration_count"),
            F.sum((~F.col("is_quarantined") & F.col("is_unusual_duration")).cast("long")).alias("silver_unusual_duration_gt_480_count"),
            F.sum((~F.col("is_quarantined") & F.col("is_negative_fare")).cast("long")).alias("silver_negative_fare_count"),
            F.sum((~F.col("is_quarantined") & F.col("is_negative_total")).cast("long")).alias("silver_negative_total_count"),
            F.sum((~F.col("is_quarantined") & F.col("is_negative_amount")).cast("long")).alias("silver_negative_amount_count"),
            F.sum((~F.col("is_quarantined") & F.col("is_missing_fare")).cast("long")).alias("silver_missing_fare_count"),
            F.sum((~F.col("is_quarantined") & F.col("is_missing_total")).cast("long")).alias("silver_missing_total_count"),
            F.sum((~F.col("is_quarantined") & F.col("is_missing_amount")).cast("long")).alias("silver_missing_amount_count"),
            F.sum((~F.col("is_quarantined") & F.col("is_passenger_missing")).cast("long")).alias("silver_missing_passenger_count"),
            F.sum((~F.col("is_quarantined") & F.col("is_missing_pickup_location")).cast("long")).alias("silver_missing_pu_location_count"),
            F.sum((~F.col("is_quarantined") & F.col("is_duplicate_record")).cast("long")).alias("silver_duplicate_record_count"),
            F.sum(F.col("is_duplicate_record").cast("long")).alias("bronze_duplicate_record_count"),
            F.sum((~legacy_kept_cond).cast("long")).alias("legacy_filter_excluded_count"),
        ).first().asDict()

        dup_excess = (
            classified
            .filter(F.col("is_duplicate_record"))
            .select("record_hash", "duplicate_group_size")
            .distinct()
            .agg(F.coalesce(F.sum(F.col("duplicate_group_size") - 1), F.lit(0)).cast("long").alias("excess"))
            .first()["excess"]
        )

        write_month_table(silver, target, "year_month", year_month)
        write_month_table(quarantine, quarantine_target, "source_year_month", year_month)

        written_silver = spark.table(target).filter(F.col("year_month") == year_month).count()
        written_quarantine = (
            spark.table(quarantine_target)
            .filter(F.col("source_year_month") == year_month)
            .count()
        )
        reconciliation_diff = raw_n - (written_silver + written_quarantine)
        reconciliation_passed = (
            written_silver == silver_n
            and written_quarantine == quarantine_n
            and reconciliation_diff == 0
        )

        audit_row = dict(audit_stats)
        audit_row.update({
            "source_year_month": year_month,
            "source_file": source_file,
            "audit_timestamp_utc": datetime.now(timezone.utc).replace(tzinfo=None),
            "written_silver_count": written_silver,
            "written_quarantine_count": written_quarantine,
            "bronze_duplicate_excess_count": int(dup_excess),
            "legacy_filter_excluded_pct": round(
                audit_stats["legacy_filter_excluded_count"] * 100.0 / raw_n, 4
            ),
            "reconciliation_diff": reconciliation_diff,
            "reconciliation_passed": reconciliation_passed,
        })
        audit_df = spark.createDataFrame([audit_row])
        write_month_table(audit_df, audit_target, "source_year_month", year_month)

        if not reconciliation_passed:
            raise ValueError(
                f"Silver reconciliation failed for {year_month}: "
                f"raw={raw_n}, expected_silver={silver_n}, written_silver={written_silver}, "
                f"expected_quarantine={quarantine_n}, written_quarantine={written_quarantine}"
            )

        print(f"--- {year_month}: Negative fare/total by payment_type (in-scope Silver) ---")
        (
            silver
            .filter(F.col("is_negative_amount"))
            .groupBy("payment_type")
            .agg(
                F.count("*").alias("trip_count"),
                F.round(F.sum("fare_amount"), 2).alias("sum_fare_usd"),
                F.round(F.sum("total_amount"), 2).alias("sum_total_usd"),
            )
            .orderBy("payment_type")
            .show(truncate=False)
        )

        if INSPECT_SAMPLES:
            sample_cols = [
                "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
                "trip_distance", "trip_duration_min", "payment_type",
                "fare_amount", "total_amount", "passenger_count",
                "quality_flags", "quarantine_reasons",
            ]
            for flag_col, label in QUALITY_FLAG_LABELS + QUARANTINE_FLAG_LABELS:
                sample_df = classified.filter(F.col(flag_col)).select(*sample_cols).limit(5)
                if sample_df.limit(1).count() > 0:
                    print(f"Sample records for {year_month} [{label}]:")
                    display(sample_df)

        return (
            year_month,
            f"in_type={in_type}  bronze={raw_n:,}  silver={written_silver:,}  "
            f"quarantine={written_quarantine:,} (in_scope={audit_stats['quarantine_in_scope_count']:,}, "
            f"out_of_month={audit_stats['quarantine_out_of_month_count']:,})  "
            f"legacy_would_drop={audit_stats['legacy_filter_excluded_count']:,} "
            f"({audit_row['legacy_filter_excluded_pct']:.2f}%)  diff={reconciliation_diff}",
        )
    finally:
        classified.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Transform, Quarantine and Audit

# CELL ********************

results = [load_month(f"{ym}-01") for ym in MONTHS]

print("---- summary ----")
for ym, msg in results:
    print(ym, msg)

(
    spark.table(audit_target)
    .filter(F.col("source_year_month").isin(MONTHS))
    .orderBy("source_year_month")
    .show(truncate=False)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
