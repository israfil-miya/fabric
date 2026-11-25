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

# ## Resolve taxi months
# Resolve one month selection for the master pipeline and validate Bronze files when needed.

# PARAMETERS CELL ********************

MONTHS_JSON = "[]"
DISCOVER_MONTHS = False
CHECK_FILES = True

# CELL ********************

import json
import re

TAXI_FOLDER = "Files/taxi"
MONTH_PATTERN = re.compile(r"[0-9]{4}-(0[1-9]|1[0-2])")
FILE_PATTERN = re.compile(
    r"yellow_tripdata_([0-9]{4}-(0[1-9]|1[0-2]))\.parquet"
)

try:
    requested_months = json.loads(MONTHS_JSON)
except (TypeError, json.JSONDecodeError) as exc:
    raise ValueError("MONTHS_JSON must be a JSON array of YYYY-MM strings") from exc

if not isinstance(DISCOVER_MONTHS, bool) or not isinstance(CHECK_FILES, bool):
    raise ValueError("DISCOVER_MONTHS and CHECK_FILES must be booleans")
if not isinstance(requested_months, list):
    raise ValueError("MONTHS_JSON must decode to an array")
if any(not isinstance(month, str) or not MONTH_PATTERN.fullmatch(month)
       for month in requested_months):
    raise ValueError("Each requested month must be a string in YYYY-MM format")
if len(requested_months) != len(set(requested_months)):
    raise ValueError("MONTHS_JSON must not contain duplicate months")
if DISCOVER_MONTHS and requested_months:
    raise ValueError("Supply either explicit months or enable discovery, not both")

files = notebookutils.fs.ls(TAXI_FOLDER) if DISCOVER_MONTHS or CHECK_FILES else []
available_by_month = {}

if DISCOVER_MONTHS:
    for file_info in files:
        name = file_info.name
        if not name.startswith("yellow_tripdata_"):
            continue
        match = FILE_PATTERN.fullmatch(name)
        if match is None:
            raise ValueError(f"Malformed taxi Parquet filename: {name}")
        month = match.group(1)
        if month in available_by_month:
            raise ValueError(f"Duplicate Bronze file found for {month}")
        if file_info.size <= 0:
            raise ValueError(f"Bronze file is empty: {name}")
        available_by_month[month] = name

    resolved_months = sorted(available_by_month)
    if not resolved_months:
        raise FileNotFoundError(f"No valid taxi Parquet files found in {TAXI_FOLDER}")
else:
    if not requested_months:
        raise ValueError("Supply months or set DISCOVER_MONTHS to true")
    resolved_months = requested_months

    if CHECK_FILES:
        files_by_name = {file_info.name: file_info for file_info in files}
        missing = [
            f"yellow_tripdata_{month}.parquet"
            for month in resolved_months
            if f"yellow_tripdata_{month}.parquet" not in files_by_name
        ]
        if missing:
            raise FileNotFoundError(
                f"Requested taxi files are missing from Bronze: {missing}"
            )
        empty = [
            name for name in (
                f"yellow_tripdata_{month}.parquet" for month in resolved_months
            )
            if files_by_name[name].size <= 0
        ]
        if empty:
            raise ValueError(f"Requested Bronze files are empty: {empty}")

print("Resolved taxi months:", resolved_months)

# CELL ********************

notebookutils.notebook.exit(json.dumps(resolved_months, separators=(",", ":")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
