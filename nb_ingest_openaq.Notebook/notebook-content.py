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
# The pipeline passes the resolved month list as JSON. Set `MONTHS_JSON` when running interactively.
# Reads the OpenAQ API key from `SECRET_FILE_PATH` in `lh_bronze` (or `OPENAQ_API_KEY` / optional `KEY_VAULT_URL`).

# PARAMETERS CELL ********************

import json
import re

MONTHS_JSON = '["2025-11","2025-12","2026-01","2026-02","2026-03"]'
SECRET_FILE_PATH = "Files/_secrets/openaq.json"
KEY_VAULT_URL = ""
KEY_VAULT_SECRET_NAME = "openaq-api-key"
OPENAQ_API_KEY = ""
SENSOR_IDS_JSON = "[673, 1097, 1102, 1103, 1152, 1128, 1145, 1146]"


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


def parse_sensor_ids(sensor_ids_json):
    try:
        ids = json.loads(sensor_ids_json)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError("SENSOR_IDS_JSON must be a JSON array of integer sensor IDs") from exc
    if not isinstance(ids, list) or not ids or not all(isinstance(x, int) and x > 0 for x in ids):
        raise ValueError("SENSOR_IDS_JSON must be a non-empty list of positive integers")
    return ids


MONTHS = parse_months(MONTHS_JSON)
SENSOR_IDS = parse_sensor_ids(SENSOR_IDS_JSON)
print("months to ingest:", MONTHS)
print("sensors to ingest:", SENSOR_IDS)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 1: Resolve OpenAQ API key and define timezone / API helpers

import json
import time as time_mod
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo
import requests
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

BASE_URL = "https://api.openaq.org/v3"
LOCAL_TIMEZONE = "America/New_York"
PAGE_LIMIT = 1000
MAX_RETRIES = 6
REQUEST_TIMEOUT_SEC = 60
POLITE_DELAY_SEC = 0.2

RAW_ROOT = "Files/air_quality/openaq/raw"
META_ROOT = "Files/air_quality/openaq/metadata"
LEGACY_LOCATION_CSV = "Files/air_quality/openaq/openaq_location_metadata.csv"


def resolve_api_key():
    if OPENAQ_API_KEY and OPENAQ_API_KEY.strip():
        print("Using OpenAQ API key provided via OPENAQ_API_KEY parameter.")
        return OPENAQ_API_KEY.strip()

    if KEY_VAULT_URL and KEY_VAULT_URL.strip():
        secret = notebookutils.credentials.getSecret(
            KEY_VAULT_URL.strip(),
            KEY_VAULT_SECRET_NAME.strip(),
        )
        if not secret:
            raise ValueError("Key Vault returned an empty OpenAQ API key.")
        print(f"Loaded OpenAQ API key from Key Vault secret '{KEY_VAULT_SECRET_NAME}'.")
        return secret.strip()

    candidate_paths = []
    for p in [SECRET_FILE_PATH, "Files/openaq.json", "Files/_secrets/openaq.json", "Files/air_quality/openaq/openaq.json"]:
        if p and p not in candidate_paths:
            candidate_paths.append(p)

    for path in candidate_paths:
        try:
            raw_text = notebookutils.fs.head(path, 65536)
        except Exception:
            continue
        raw_text = raw_text.strip()
        if not raw_text:
            continue
        try:
            payload = json.loads(raw_text)
        except json.JSONDecodeError:
            # Allow plain-text single-key file if user uploaded key directly
            print(f"Loaded OpenAQ API key from plain-text file '{path}'.")
            return raw_text
        if isinstance(payload, dict):
            for key_name in ("openaq_api_key", "api_key", "OPENAQ_API_KEY", "x-api-key", "key"):
                val = payload.get(key_name)
                if isinstance(val, str) and val.strip():
                    print(f"Loaded OpenAQ API key from '{path}' ({key_name}).")
                    return val.strip()
        raise ValueError(
            f"Secret file '{path}' did not contain 'openaq_api_key' (or 'api_key')."
        )

    raise FileNotFoundError(
        "OpenAQ API key not found. Upload JSON {\"openaq_api_key\": \"...\"} to "
        f"{SECRET_FILE_PATH} (or Files/openaq.json), or supply OPENAQ_API_KEY / KEY_VAULT_URL."
    )


API_KEY = resolve_api_key()
SESSION = requests.Session()
SESSION.headers.update({
    "X-API-Key": API_KEY,
    "Accept": "application/json",
})


def next_month(ms):
    y, m = int(ms[:4]), int(ms[5:7])
    return f"{y + (m == 12)}-{(m % 12) + 1:02d}-01"


def month_local_bounds_utc(ym):
    """Return (start_utc, end_utc) as UTC datetimes for America/New_York calendar month YYYY-MM."""
    ny_tz = ZoneInfo(LOCAL_TIMEZONE)
    start_d = date.fromisoformat(f"{ym}-01")
    end_d = date.fromisoformat(next_month(f"{ym}-01"))
    start_local = datetime.combine(start_d, time.min, tzinfo=ny_tz)
    end_local = datetime.combine(end_d, time.min, tzinfo=ny_tz)
    return (
        start_local.astimezone(timezone.utc),
        end_local.astimezone(timezone.utc),
    )


def parse_utc_iso(ts_str):
    if not ts_str or not isinstance(ts_str, str):
        return None
    normalized = ts_str.strip().replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def api_get_json(endpoint, params=None):
    url = f"{BASE_URL}/{endpoint.lstrip('/')}"
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = SESSION.get(url, params=params, timeout=REQUEST_TIMEOUT_SEC)
            if resp.status_code == 200:
                if POLITE_DELAY_SEC > 0:
                    time_mod.sleep(POLITE_DELAY_SEC)
                return resp.json()
            if resp.status_code in (429, 500, 502, 503, 504):
                retry_after = resp.headers.get("Retry-After")
                wait_sec = float(retry_after) if retry_after and retry_after.isdigit() else min(2 ** attempt, 60)
                print(
                    f"Transient HTTP {resp.status_code} on {endpoint} (attempt {attempt}/{MAX_RETRIES}); "
                    f"retrying in {wait_sec:.1f}s"
                )
                time_mod.sleep(wait_sec)
                continue
            raise RuntimeError(
                f"OpenAQ API request failed ({resp.status_code}) for {url} params={params}: "
                f"{resp.text[:500]}"
            )
        except requests.RequestException as exc:
            last_exc = exc
            wait_sec = min(2 ** attempt, 60)
            print(
                f"Network error on {endpoint} (attempt {attempt}/{MAX_RETRIES}): {exc}; "
                f"retrying in {wait_sec:.1f}s"
            )
            time_mod.sleep(wait_sec)
    raise RuntimeError(
        f"OpenAQ API request failed after {MAX_RETRIES} attempts for {url} params={params}: {last_exc}"
    )


def write_json_file(path, data_obj):
    parent = path.rsplit("/", 1)[0]
    notebookutils.fs.mkdirs(parent)
    notebookutils.fs.put(path, json.dumps(data_obj, ensure_ascii=False), True)


def clear_raw_pages(folder_path):
    try:
        entries = notebookutils.fs.ls(folder_path)
    except Exception:
        return
    for entry in entries:
        if entry.name.startswith("page_") and entry.name.endswith(".json"):
            notebookutils.fs.rm(entry.path, False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 2: Fetch sensor and location metadata (preserving raw JSON)

ingested_at_utc = datetime.now(timezone.utc).replace(tzinfo=None)

sensor_rows = []
for sid in SENSOR_IDS:
    payload = api_get_json(f"sensors/{sid}")
    write_json_file(f"{META_ROOT}/sensors/sensor_{sid}.json", payload)
    results = payload.get("results") or []
    if not results:
        raise ValueError(f"OpenAQ /v3/sensors/{sid} returned no results.")
    item = results[0]
    param = item.get("parameter") or {}
    cov = item.get("coverage") or {}
    dt_first = item.get("datetimeFirst") or {}
    dt_last = item.get("datetimeLast") or {}
    sensor_rows.append({
        "sensor_id": int(item.get("id") or sid),
        "sensor_name": item.get("name"),
        "parameter": param.get("name"),
        "unit": param.get("units"),
        "first_observation_utc": dt_first.get("utc"),
        "last_observation_utc": dt_last.get("utc"),
        "expected_count": int(cov["expectedCount"]) if cov.get("expectedCount") is not None else None,
        "actual_count": int(cov["actualCount"]) if cov.get("actualCount") is not None else None,
        "percent_complete": float(cov["percentComplete"]) if cov.get("percentComplete") is not None else None,
        "ingested_at_utc": ingested_at_utc,
    })

# Discover NYC locations from OpenAQ v3 so sensor -> location metadata stays current.
nyc_location_pages = []
sensor_to_location = {}
page = 1
while True:
    loc_payload = api_get_json(
        "locations",
        params={"coordinates": "40.7128,-74.0060", "radius": 25000, "parameters_id": 2, "limit": PAGE_LIMIT, "page": page},
    )
    nyc_location_pages.append(loc_payload)
    write_json_file(f"{META_ROOT}/locations/nyc_locations_page_{page:04d}.json", loc_payload)
    results = loc_payload.get("results") or []
    for loc in results:
        for s in (loc.get("sensors") or []):
            s_id = s.get("id")
            if s_id is not None:
                sensor_to_location[int(s_id)] = loc
    if len(results) < PAGE_LIMIT:
        break
    page += 1

# Fallback: if any selected sensor fell outside the 25km NYC query, look up its location_id from existing CSV if present.
missing_loc_sensors = [sid for sid in SENSOR_IDS if sid not in sensor_to_location]
if missing_loc_sensors:
    try:
        legacy_loc_df = (
            spark.read.format("csv").option("header", "true").load(LEGACY_LOCATION_CSV)
            .select(F.col("sensor_id").cast("long").alias("sensor_id"), F.col("location_id").cast("long").alias("location_id"))
            .filter(F.col("sensor_id").isin(missing_loc_sensors))
            .collect()
        )
        for row in legacy_loc_df:
            loc_id = row["location_id"]
            if loc_id is not None:
                loc_detail = api_get_json(f"locations/{loc_id}")
                write_json_file(f"{META_ROOT}/locations/location_{loc_id}.json", loc_detail)
                for loc in (loc_detail.get("results") or []):
                    sensor_to_location[int(row["sensor_id"])] = loc
    except Exception as exc:
        print(f"Fallback location lookup skipped/failed for {missing_loc_sensors}: {exc}")

still_missing_locs = [sid for sid in SENSOR_IDS if sid not in sensor_to_location]
if still_missing_locs:
    raise ValueError(f"Could not resolve location metadata for selected sensors: {still_missing_locs}")

location_rows = []
for sid in SENSOR_IDS:
    loc = sensor_to_location[sid]
    coords = loc.get("coordinates") or {}
    country = loc.get("country") or {}
    provider = loc.get("provider") or {}
    owner = loc.get("owner") or {}
    location_rows.append({
        "sensor_id": int(sid),
        "location_id": int(loc["id"]),
        "location_name": loc.get("name"),
        "locality": loc.get("locality"),
        "country": country.get("code") or country.get("name"),
        "timezone": loc.get("timezone"),
        "provider_name": provider.get("name"),
        "owner_name": owner.get("name"),
        "is_monitor": bool(loc["isMonitor"]) if loc.get("isMonitor") is not None else None,
        "is_mobile": bool(loc["isMobile"]) if loc.get("isMobile") is not None else None,
        "latitude": float(coords["latitude"]) if coords.get("latitude") is not None else None,
        "longitude": float(coords["longitude"]) if coords.get("longitude") is not None else None,
        "instruments": json.dumps(loc.get("instruments") or [], ensure_ascii=False),
        "licenses": json.dumps(loc.get("licenses") or [], ensure_ascii=False),
        "ingested_at_utc": ingested_at_utc,
    })

print(f"Fetched metadata for {len(sensor_rows)} sensors and {len(location_rows)} sensor-location pairs.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 3: Fetch hourly measurements and sensor flags per sensor and month (preserving raw JSON)

hourly_rows = []
flag_rows = []
audit_rows = []

for ym in MONTHS:
    start_utc, end_utc = month_local_bounds_utc(ym)
    # Query through end_utc because OpenAQ /hours filters period end (datetimeTo <= datetime_to);
    # the Python check (start_utc <= from_utc_dt < end_utc) keeps exact half-open month boundaries.
    query_from = start_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    query_to = end_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    for sid in SENSOR_IDS:
        loc_id = int(sensor_to_location[sid]["id"])
        loc_name = sensor_to_location[sid].get("name")
        raw_folder = f"{RAW_ROOT}/sensor_id={sid}/year_month={ym}"
        clear_raw_pages(raw_folder)

        page = 1
        pages_fetched = 0
        api_total_records = 0
        in_month_records = 0
        out_of_window_records = 0
        seen_hours_for_sensor_month = set()

        while True:
            payload = api_get_json(
                f"sensors/{sid}/hours",
                params={
                    "datetime_from": query_from,
                    "datetime_to": query_to,
                    "limit": PAGE_LIMIT,
                    "page": page,
                },
            )
            write_json_file(f"{raw_folder}/page_{page:04d}.json", payload)
            pages_fetched += 1

            results = payload.get("results") or []
            api_total_records += len(results)

            for item in results:
                period = item.get("period") or {}
                dt_from = period.get("datetimeFrom") or item.get("datetimeFrom") or {}
                dt_to = period.get("datetimeTo") or item.get("datetimeTo") or {}
                from_utc_str = dt_from.get("utc")
                from_utc_dt = parse_utc_iso(from_utc_str)

                if from_utc_dt is None or not (start_utc <= from_utc_dt < end_utc):
                    out_of_window_records += 1
                    continue

                if from_utc_str in seen_hours_for_sensor_month:
                    raise ValueError(
                        f"Duplicate hourly timestamp '{from_utc_str}' returned by API for "
                        f"sensor={sid}, month={ym}, page={page}"
                    )
                seen_hours_for_sensor_month.add(from_utc_str)
                in_month_records += 1

                param = item.get("parameter") or {}
                summary = item.get("summary") or {}
                cov = item.get("coverage") or {}
                flag_info = cov.get("flagInfo") if isinstance(cov.get("flagInfo"), dict) else item.get("flagInfo")
                has_flags_val = flag_info.get("hasFlags") if isinstance(flag_info, dict) else None
                if has_flags_val is not None:
                    has_flags_val = bool(has_flags_val)

                hourly_rows.append({
                    "sensor_id": int(sid),
                    "location_id": loc_id,
                    "location_name": loc_name,
                    "value": float(item["value"]) if item.get("value") is not None else None,
                    "parameter": param.get("name"),
                    "unit": param.get("units"),
                    "period_label": period.get("label"),
                    "period_interval": period.get("interval"),
                    "datetime_from_utc": from_utc_str,
                    "datetime_to_utc": dt_to.get("utc"),
                    "datetime_from_local": dt_from.get("local"),
                    "datetime_to_local": dt_to.get("local"),
                    "summary_min": float(summary["min"]) if summary.get("min") is not None else None,
                    "summary_max": float(summary["max"]) if summary.get("max") is not None else None,
                    "summary_mean": float(summary["mean"]) if summary.get("mean") is not None else None,
                    "summary_median": float(summary["median"]) if summary.get("median") is not None else None,
                    "summary_sd": float(summary["sd"]) if summary.get("sd") is not None else None,
                    "coverage_expected_count": int(cov["expectedCount"]) if cov.get("expectedCount") is not None else None,
                    "coverage_observed_count": int(cov["observedCount"]) if cov.get("observedCount") is not None else None,
                    "coverage_percent_complete": float(cov["percentComplete"]) if cov.get("percentComplete") is not None else None,
                    "coverage_percent_coverage": float(cov["percentCoverage"]) if cov.get("percentCoverage") is not None else None,
                    "has_flags": has_flags_val,
                    "year_month": ym,
                    "page_number": int(page),
                    "ingested_at_utc": ingested_at_utc,
                })

            if len(results) < PAGE_LIMIT:
                break
            page += 1

        if api_total_records != in_month_records + out_of_window_records:
            raise ValueError(
                f"API page reconciliation mismatch for sensor={sid}, month={ym}: "
                f"api_total={api_total_records}, in_month={in_month_records}, out={out_of_window_records}"
            )

        audit_rows.append({
            "year_month": ym,
            "sensor_id": int(sid),
            "month_start_utc": start_utc.replace(tzinfo=None),
            "month_end_utc": end_utc.replace(tzinfo=None),
            "api_pages_fetched": int(pages_fetched),
            "api_total_records": int(api_total_records),
            "in_month_records": int(in_month_records),
            "out_of_window_records": int(out_of_window_records),
            "written_delta_records": int(in_month_records),
            "status": "SUCCESS",
            "ingested_at_utc": ingested_at_utc,
        })

        print(
            f"[{ym}] sensor={sid}: pages={pages_fetched}, api_total={api_total_records:,}, "
            f"in_month={in_month_records:,}, out_of_window={out_of_window_records:,}"
        )

# Fetch sensor flags covering the overall min..max UTC span of MONTHS (including ongoing flags).
min_start_utc = min(month_local_bounds_utc(ym)[0] for ym in MONTHS)
max_end_utc = max(month_local_bounds_utc(ym)[1] for ym in MONTHS)
flags_from = min_start_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
flags_to = max_end_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

for sid in SENSOR_IDS:
    page = 1
    while True:
        flag_payload = api_get_json(
            f"sensors/{sid}/flags",
            params={"datetime_from": flags_from, "datetime_to": flags_to, "limit": PAGE_LIMIT, "page": page},
        )
        write_json_file(f"{META_ROOT}/flags/sensor_{sid}_page_{page:04d}.json", flag_payload)
        results = flag_payload.get("results") or []
        for f_item in results:
            ft = f_item.get("flagType") or {}
            dt_from = f_item.get("datetimeFrom") or {}
            dt_to = f_item.get("datetimeTo") or {}
            flag_rows.append({
                "queried_sensor_id": int(sid),
                "location_id": int(f_item["locationId"]) if f_item.get("locationId") is not None else int(sensor_to_location[sid]["id"]),
                "flag_type_id": int(ft["id"]) if ft.get("id") is not None else None,
                "flag_label": ft.get("label"),
                "flag_level": ft.get("level"),
                "datetime_from_utc": dt_from.get("utc"),
                "datetime_to_utc": dt_to.get("utc"),
                "datetime_from_local": dt_from.get("local"),
                "datetime_to_local": dt_to.get("local"),
                "sensor_ids": json.dumps(f_item.get("sensorIds") or [], ensure_ascii=False),
                "note": f_item.get("note"),
                "ingested_at_utc": ingested_at_utc,
            })
        if len(results) < PAGE_LIMIT:
            break
        page += 1

print(f"Total normalized hourly rows across MONTHS: {len(hourly_rows):,}")
print(f"Total fetched flag events: {len(flag_rows):,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 4: Write normalized Bronze Delta tables and validate counts

HOURLY_SCHEMA = StructType([
    StructField("sensor_id", LongType(), False),
    StructField("location_id", LongType(), True),
    StructField("location_name", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("parameter", StringType(), True),
    StructField("unit", StringType(), True),
    StructField("period_label", StringType(), True),
    StructField("period_interval", StringType(), True),
    StructField("datetime_from_utc", StringType(), True),
    StructField("datetime_to_utc", StringType(), True),
    StructField("datetime_from_local", StringType(), True),
    StructField("datetime_to_local", StringType(), True),
    StructField("summary_min", DoubleType(), True),
    StructField("summary_max", DoubleType(), True),
    StructField("summary_mean", DoubleType(), True),
    StructField("summary_median", DoubleType(), True),
    StructField("summary_sd", DoubleType(), True),
    StructField("coverage_expected_count", LongType(), True),
    StructField("coverage_observed_count", LongType(), True),
    StructField("coverage_percent_complete", DoubleType(), True),
    StructField("coverage_percent_coverage", DoubleType(), True),
    StructField("has_flags", BooleanType(), True),
    StructField("year_month", StringType(), False),
    StructField("page_number", LongType(), False),
    StructField("ingested_at_utc", TimestampType(), False),
])

SENSOR_SCHEMA = StructType([
    StructField("sensor_id", LongType(), False),
    StructField("sensor_name", StringType(), True),
    StructField("parameter", StringType(), True),
    StructField("unit", StringType(), True),
    StructField("first_observation_utc", StringType(), True),
    StructField("last_observation_utc", StringType(), True),
    StructField("expected_count", LongType(), True),
    StructField("actual_count", LongType(), True),
    StructField("percent_complete", DoubleType(), True),
    StructField("ingested_at_utc", TimestampType(), False),
])

LOCATION_SCHEMA = StructType([
    StructField("sensor_id", LongType(), False),
    StructField("location_id", LongType(), False),
    StructField("location_name", StringType(), True),
    StructField("locality", StringType(), True),
    StructField("country", StringType(), True),
    StructField("timezone", StringType(), True),
    StructField("provider_name", StringType(), True),
    StructField("owner_name", StringType(), True),
    StructField("is_monitor", BooleanType(), True),
    StructField("is_mobile", BooleanType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("instruments", StringType(), True),
    StructField("licenses", StringType(), True),
    StructField("ingested_at_utc", TimestampType(), False),
])

FLAGS_SCHEMA = StructType([
    StructField("queried_sensor_id", LongType(), False),
    StructField("location_id", LongType(), True),
    StructField("flag_type_id", LongType(), True),
    StructField("flag_label", StringType(), True),
    StructField("flag_level", StringType(), True),
    StructField("datetime_from_utc", StringType(), True),
    StructField("datetime_to_utc", StringType(), True),
    StructField("datetime_from_local", StringType(), True),
    StructField("datetime_to_local", StringType(), True),
    StructField("sensor_ids", StringType(), True),
    StructField("note", StringType(), True),
    StructField("ingested_at_utc", TimestampType(), False),
])

AUDIT_SCHEMA = StructType([
    StructField("year_month", StringType(), False),
    StructField("sensor_id", LongType(), False),
    StructField("month_start_utc", TimestampType(), False),
    StructField("month_end_utc", TimestampType(), False),
    StructField("api_pages_fetched", LongType(), False),
    StructField("api_total_records", LongType(), False),
    StructField("in_month_records", LongType(), False),
    StructField("out_of_window_records", LongType(), False),
    StructField("written_delta_records", LongType(), False),
    StructField("status", StringType(), False),
    StructField("ingested_at_utc", TimestampType(), False),
])

sensor_df = spark.createDataFrame(sensor_rows, schema=SENSOR_SCHEMA)
location_df = spark.createDataFrame(location_rows, schema=LOCATION_SCHEMA)
flags_df = spark.createDataFrame(flag_rows, schema=FLAGS_SCHEMA)
hourly_df = spark.createDataFrame(hourly_rows, schema=HOURLY_SCHEMA)
audit_df = spark.createDataFrame(audit_rows, schema=AUDIT_SCHEMA)

(
    sensor_df.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("dbo.bronze_openaq_sensor")
)
(
    location_df.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("dbo.bronze_openaq_location")
)

# Merge flag events by business key so reprocessing one month preserves earlier flag history.
FLAGS_TABLE = "dbo.bronze_openaq_flags"
if spark.catalog.tableExists(FLAGS_TABLE):
    existing_flags = spark.table(FLAGS_TABLE)
    merged_flags = (
        existing_flags.unionByName(flags_df, allowMissingColumns=True)
        .orderBy(F.desc("ingested_at_utc"))
        .dropDuplicates([
            "queried_sensor_id", "location_id", "flag_type_id",
            "flag_label", "datetime_from_utc", "datetime_to_utc",
        ])
    )
else:
    merged_flags = flags_df

(
    merged_flags.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(FLAGS_TABLE)
)

HOURLY_TABLE = "dbo.bronze_openaq_hourly"
AUDIT_TABLE = "dbo.bronze_openaq_ingest_audit"

for ym in MONTHS:
    m_hourly = hourly_df.filter(F.col("year_month") == ym)
    m_audit = audit_df.filter(F.col("year_month") == ym)

    if spark.catalog.tableExists(HOURLY_TABLE):
        (
            m_hourly.write.format("delta").mode("overwrite")
            .option("replaceWhere", f"year_month = '{ym}'")
            .option("mergeSchema", "true")
            .saveAsTable(HOURLY_TABLE)
        )
    else:
        m_hourly.write.format("delta").saveAsTable(HOURLY_TABLE)

    if spark.catalog.tableExists(AUDIT_TABLE):
        (
            m_audit.write.format("delta").mode("overwrite")
            .option("replaceWhere", f"year_month = '{ym}'")
            .option("mergeSchema", "true")
            .saveAsTable(AUDIT_TABLE)
        )
    else:
        m_audit.write.format("delta").saveAsTable(AUDIT_TABLE)

    expected_n = sum(r["in_month_records"] for r in audit_rows if r["year_month"] == ym)
    written_n = spark.table(HOURLY_TABLE).filter(F.col("year_month") == ym).count()
    if written_n != expected_n:
        raise ValueError(
            f"Bronze hourly write mismatch for {ym}: expected {expected_n}, wrote {written_n}"
        )
    print(f"Verified bronze_openaq_hourly [{ym}]: {written_n:,} records")

spark.table(AUDIT_TABLE).filter(F.col("year_month").isin(MONTHS)).orderBy("year_month", "sensor_id").show(100, truncate=False)
print("ALL CHECKS PASSED")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
