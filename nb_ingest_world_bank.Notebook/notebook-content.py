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
# Derives the World Bank GDP year window from `MONTHS_JSON` (with lookback years for annual context).

# PARAMETERS CELL ********************

import json
import re

MONTHS_JSON = "[]"
COUNTRIES_JSON = '["USA", "EMU"]'
INDICATOR = "NY.GDP.MKTP.CD"
LOOKBACK_YEARS = 5


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


def parse_countries(countries_json):
    try:
        countries = json.loads(countries_json)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError("COUNTRIES_JSON must be a JSON array of ISO3/WB country codes") from exc
    if not isinstance(countries, list) or not countries or not all(
        isinstance(c, str) and c.strip() for c in countries
    ):
        raise ValueError("COUNTRIES_JSON must be a non-empty array of country codes")
    return [c.strip().upper() for c in countries]


MONTHS = parse_months(MONTHS_JSON)
COUNTRIES = parse_countries(COUNTRIES_JSON)
print("months context:", MONTHS)
print("countries:", COUNTRIES, "| indicator:", INDICATOR)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 1: Fetch World Bank GDP JSON with retries and raw preservation

import json
import time as time_mod
from datetime import datetime, timezone
import requests
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

BASE_URL = "https://api.worldbank.org/v2"
RAW_ROOT = "Files/economics/world_bank/raw"
MAX_RETRIES = 5
PER_PAGE = 1000

selected_years = sorted({int(ym[:4]) for ym in MONTHS})
start_year = min(selected_years) - int(LOOKBACK_YEARS)
end_year = max(selected_years)
date_range = f"{start_year}:{end_year}"
countries_param = ";".join(COUNTRIES)
ingested_at_utc = datetime.now(timezone.utc).replace(tzinfo=None)


def fetch_wb_page(page):
    url = f"{BASE_URL}/country/{countries_param}/indicator/{INDICATOR}"
    params = {"format": "json", "date": date_range, "per_page": PER_PAGE, "page": page}
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, timeout=60)
            if resp.status_code == 200:
                payload = resp.json()
                if not isinstance(payload, list) or len(payload) < 2 or not isinstance(payload[0], dict):
                    raise RuntimeError(f"Unexpected World Bank payload: {str(payload)[:400]}")
                return payload
            if resp.status_code in (429, 500, 502, 503, 504):
                wait_sec = min(2 ** attempt, 30)
                print(f"Transient HTTP {resp.status_code} (attempt {attempt}/{MAX_RETRIES}); retrying in {wait_sec}s")
                time_mod.sleep(wait_sec)
                continue
            raise RuntimeError(f"World Bank API error ({resp.status_code}): {resp.text[:400]}")
        except requests.RequestException as exc:
            last_exc = exc
            wait_sec = min(2 ** attempt, 30)
            print(f"Network error (attempt {attempt}/{MAX_RETRIES}): {exc}; retrying in {wait_sec}s")
            time_mod.sleep(wait_sec)
    raise RuntimeError(f"World Bank API failed after {MAX_RETRIES} attempts: {last_exc}")


raw_folder = f"{RAW_ROOT}/indicator={INDICATOR}/years={start_year}_{end_year}"
notebookutils.fs.mkdirs(raw_folder)

all_records = []
api_total_declared = None
page = 1
while True:
    payload = fetch_wb_page(page)
    notebookutils.fs.put(
        f"{raw_folder}/page_{page:04d}.json",
        json.dumps(payload, ensure_ascii=False),
        True,
    )
    meta, records = payload[0], payload[1] or []
    if api_total_declared is None:
        api_total_declared = int(meta.get("total", len(records)))
    total_pages = int(meta.get("pages", 1))
    all_records.extend(records)
    print(f"Page {page}/{total_pages}: fetched {len(records)} records (declared total={api_total_declared})")
    if page >= total_pages:
        break
    page += 1

if len(all_records) != api_total_declared:
    raise ValueError(
        f"World Bank API pagination mismatch: declared total={api_total_declared}, fetched={len(all_records)}"
    )

normalized_rows = []
for rec in all_records:
    ind = rec.get("indicator") or {}
    ctry = rec.get("country") or {}
    val = rec.get("value")
    normalized_rows.append({
        "indicator_id": ind.get("id") or INDICATOR,
        "indicator_name": ind.get("value"),
        "country_code_2": ctry.get("id"),
        "country_name": ctry.get("value"),
        "country_iso3": rec.get("countryiso3code"),
        "year": int(rec["date"]) if rec.get("date") and str(rec["date"]).isdigit() else None,
        "year_raw": str(rec.get("date")) if rec.get("date") is not None else None,
        "value": float(val) if val is not None else None,
        "unit": rec.get("unit"),
        "obs_status": rec.get("obs_status"),
        "decimal": int(rec["decimal"]) if rec.get("decimal") is not None else None,
        "last_updated_date": meta.get("lastupdated"),
        "ingested_at_utc": ingested_at_utc,
    })

GDP_SCHEMA = StructType([
    StructField("indicator_id", StringType(), False),
    StructField("indicator_name", StringType(), True),
    StructField("country_code_2", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("country_iso3", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("year_raw", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("unit", StringType(), True),
    StructField("obs_status", StringType(), True),
    StructField("decimal", IntegerType(), True),
    StructField("last_updated_date", StringType(), True),
    StructField("ingested_at_utc", TimestampType(), False),
])

BRONZE_TABLE = "dbo.bronze_world_bank_gdp"
new_df = spark.createDataFrame(normalized_rows, schema=GDP_SCHEMA)

if spark.catalog.tableExists(BRONZE_TABLE):
    existing_df = spark.table(BRONZE_TABLE)
    merged_df = (
        existing_df.unionByName(new_df, allowMissingColumns=True)
        .orderBy(F.desc("ingested_at_utc"))
        .dropDuplicates(["indicator_id", "country_iso3", "year_raw"])
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
        (F.col("indicator_id") == INDICATOR)
        & F.col("country_iso3").isin(COUNTRIES)
        & F.col("year").between(start_year, end_year)
    )
    .count()
)
if written_window_n != len(normalized_rows):
    raise ValueError(
        f"World Bank Bronze write mismatch: fetched={len(normalized_rows)}, written_in_window={written_window_n}"
    )

AUDIT_SCHEMA = StructType([
    StructField("indicator_id", StringType(), False),
    StructField("countries", StringType(), False),
    StructField("start_year", IntegerType(), False),
    StructField("end_year", IntegerType(), False),
    StructField("api_total_declared", LongType(), False),
    StructField("api_records_fetched", LongType(), False),
    StructField("written_window_records", LongType(), False),
    StructField("null_value_records", LongType(), False),
    StructField("status", StringType(), False),
    StructField("ingested_at_utc", TimestampType(), False),
])

null_vals = sum(1 for r in normalized_rows if r["value"] is None)
audit_row = [{
    "indicator_id": INDICATOR,
    "countries": countries_param,
    "start_year": int(start_year),
    "end_year": int(end_year),
    "api_total_declared": int(api_total_declared),
    "api_records_fetched": int(len(normalized_rows)),
    "written_window_records": int(written_window_n),
    "null_value_records": int(null_vals),
    "status": "SUCCESS",
    "ingested_at_utc": ingested_at_utc,
}]
(
    spark.createDataFrame(audit_row, schema=AUDIT_SCHEMA)
    .write.format("delta").mode("append")
    .option("mergeSchema", "true")
    .saveAsTable("dbo.bronze_world_bank_ingest_audit")
)

spark.table(BRONZE_TABLE).orderBy("country_iso3", "year").show(50, truncate=False)
print("ALL CHECKS PASSED")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
