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

# ## Configuration
# Derives the ECB daily exchange-rate window from `MONTHS_JSON` (with a 14-day lookback buffer so weekend/holiday month starts always have a prior business-day rate).

# PARAMETERS CELL ********************

import json
import re

MONTHS_JSON = '["2025-11","2025-12","2026-01","2026-02","2026-03"]'
CURRENCY_SERIES = "D.USD.EUR.SP00.A"
LOOKBACK_DAYS = 14


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
print("ECB series:", CURRENCY_SERIES)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 1: Fetch ECB daily FX series CSV, preserve raw file, and write normalized Bronze Delta

import csv
import io
import time as time_mod
from datetime import date, datetime, timedelta, timezone
import requests
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

BASE_URL = "https://data-api.ecb.europa.eu/service/data/EXR"
RAW_ROOT = "Files/economics/ecb_fx/raw"
MAX_RETRIES = 5


def next_month_start(ym):
    y, m = int(ym[:4]), int(ym[5:7])
    return date(y + (m == 12), (m % 12) + 1, 1)


min_date = min(date.fromisoformat(f"{ym}-01") for ym in MONTHS)
max_date = max(next_month_start(ym) - timedelta(days=1) for ym in MONTHS)
fetch_start = min_date - timedelta(days=int(LOOKBACK_DAYS))
fetch_end = max_date
ingested_at_utc = datetime.now(timezone.utc).replace(tzinfo=None)


def fetch_ecb_csv(start_period, end_period):
    url = f"{BASE_URL}/{CURRENCY_SERIES}"
    params = {"startPeriod": start_period, "endPeriod": end_period}
    headers = {"Accept": "text/csv"}
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=60)
            if resp.status_code == 200:
                if not resp.text.strip():
                    raise RuntimeError("ECB Data API returned an empty CSV payload.")
                return resp.text
            if resp.status_code in (429, 500, 502, 503, 504):
                wait_sec = min(2 ** attempt, 30)
                print(f"Transient HTTP {resp.status_code} (attempt {attempt}/{MAX_RETRIES}); retrying in {wait_sec}s")
                time_mod.sleep(wait_sec)
                continue
            raise RuntimeError(f"ECB Data API error ({resp.status_code}): {resp.text[:500]}")
        except requests.RequestException as exc:
            last_exc = exc
            wait_sec = min(2 ** attempt, 30)
            print(f"Network error (attempt {attempt}/{MAX_RETRIES}): {exc}; retrying in {wait_sec}s")
            time_mod.sleep(wait_sec)
    raise RuntimeError(f"ECB Data API failed after {MAX_RETRIES} attempts: {last_exc}")


csv_text = fetch_ecb_csv(fetch_start.isoformat(), fetch_end.isoformat())

raw_folder = f"{RAW_ROOT}/series={CURRENCY_SERIES}"
raw_file = f"{raw_folder}/period={fetch_start.isoformat()}_{fetch_end.isoformat()}.csv"
notebookutils.fs.mkdirs(raw_folder)
notebookutils.fs.put(raw_file, csv_text, True)
print(f"Preserved raw ECB CSV at {raw_file}")

reader = csv.DictReader(io.StringIO(csv_text))
raw_rows = list(reader)
if not raw_rows:
    raise ValueError(f"No observations returned by ECB for {fetch_start}..{fetch_end}")

normalized_rows = []
for r in raw_rows:
    base_ccy = (r.get("CURRENCY_DENOM") or "EUR").strip()
    quote_ccy = (r.get("CURRENCY") or "USD").strip()
    obs_val_raw = (r.get("OBS_VALUE") or "").strip()
    obs_val = float(obs_val_raw) if obs_val_raw not in ("", "NaN", "nan") else None
    normalized_rows.append({
        "series_key": (r.get("KEY") or f"EXR.{CURRENCY_SERIES}").strip(),
        "observation_date_str": (r.get("TIME_PERIOD") or "").strip(),
        "currency_pair": f"{base_ccy}/{quote_ccy}",
        "base_currency": base_ccy,
        "quote_currency": quote_ccy,
        "quote_direction": f"{quote_ccy} per 1 {base_ccy}",
        "exchange_rate_value": obs_val,
        "exchange_rate_raw": obs_val_raw,
        "unit": (r.get("UNIT") or "").strip() or None,
        "unit_mult": (r.get("UNIT_MULT") or "").strip() or None,
        "obs_status": (r.get("OBS_STATUS") or "").strip() or None,
        "title": (r.get("TITLE") or "").strip() or None,
        "ingested_at_utc": ingested_at_utc,
    })

FX_SCHEMA = StructType([
    StructField("series_key", StringType(), False),
    StructField("observation_date_str", StringType(), False),
    StructField("currency_pair", StringType(), False),
    StructField("base_currency", StringType(), False),
    StructField("quote_currency", StringType(), False),
    StructField("quote_direction", StringType(), False),
    StructField("exchange_rate_value", DoubleType(), True),
    StructField("exchange_rate_raw", StringType(), True),
    StructField("unit", StringType(), True),
    StructField("unit_mult", StringType(), True),
    StructField("obs_status", StringType(), True),
    StructField("title", StringType(), True),
    StructField("ingested_at_utc", TimestampType(), False),
])

BRONZE_TABLE = "dbo.bronze_ecb_fx"
new_df = spark.createDataFrame(normalized_rows, schema=FX_SCHEMA)

if spark.catalog.tableExists(BRONZE_TABLE):
    existing_df = spark.table(BRONZE_TABLE)
    merged_df = (
        existing_df.unionByName(new_df, allowMissingColumns=True)
        .orderBy(F.desc("ingested_at_utc"))
        .dropDuplicates(["series_key", "observation_date_str"])
    )
else:
    merged_df = new_df

(
    merged_df.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(BRONZE_TABLE)
)

written_window_n = (
    spark.table(BRONZE_TABLE)
    .filter(
        (F.col("observation_date_str") >= fetch_start.isoformat())
        & (F.col("observation_date_str") <= fetch_end.isoformat())
    )
    .count()
)
if written_window_n != len(normalized_rows):
    raise ValueError(
        f"ECB Bronze write mismatch: parsed={len(normalized_rows)}, written_in_window={written_window_n}"
    )

AUDIT_SCHEMA = StructType([
    StructField("series_key", StringType(), False),
    StructField("fetch_start_date", StringType(), False),
    StructField("fetch_end_date", StringType(), False),
    StructField("raw_csv_rows", LongType(), False),
    StructField("parsed_rows", LongType(), False),
    StructField("written_window_rows", LongType(), False),
    StructField("null_rate_rows", LongType(), False),
    StructField("status", StringType(), False),
    StructField("ingested_at_utc", TimestampType(), False),
])

null_rates = sum(1 for r in normalized_rows if r["exchange_rate_value"] is None)
audit_row = [{
    "series_key": f"EXR.{CURRENCY_SERIES}",
    "fetch_start_date": fetch_start.isoformat(),
    "fetch_end_date": fetch_end.isoformat(),
    "raw_csv_rows": int(len(raw_rows)),
    "parsed_rows": int(len(normalized_rows)),
    "written_window_rows": int(written_window_n),
    "null_rate_rows": int(null_rates),
    "status": "SUCCESS",
    "ingested_at_utc": ingested_at_utc,
}]
(
    spark.createDataFrame(audit_row, schema=AUDIT_SCHEMA)
    .write.format("delta").mode("append")
    .option("mergeSchema", "true")
    .saveAsTable("dbo.bronze_ecb_fx_ingest_audit")
)

print(
    f"ECB FX ingested: window={fetch_start}..{fetch_end}, "
    f"raw_csv_rows={len(raw_rows):,}, written_in_window={written_window_n:,}, null_rates={null_rates}"
)
print("ALL CHECKS PASSED")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
