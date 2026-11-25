# Data Dictionary & Source-to-Target Lineage

This document maps how raw data moves through `lh_bronze`, `lh_silver`, `lh_gold`, and the SQL views in `wh_nyc_analytics`, along with the grain and schema decisions behind each table.

## 1. Source-to-Target Lineage

Raw files land in `lh_bronze/Files` for traceability alongside normalized Bronze Delta tables for API sources. From Silver onward, every table enforces explicit types, keeps anomalies visible via flags or quarantine tables, and reconciles back to its source count.

| Source | Bronze Storage / Table (`lh_bronze`) | Silver Table (`lh_silver`) | Gold / Warehouse Table (`lh_gold` / `wh_nyc_analytics`) |
| --- | --- | --- | --- |
| NYC TLC Yellow Taxi Parquet (`yellow_tripdata_YYYY-MM.parquet`) | `Files/taxi/yellow_tripdata_YYYY-MM.parquet` | `silver_taxi`, `silver_taxi_quarantine`, `silver_taxi_quality_audit` | `dim_date`, `fact_taxi_daily`, `fact_taxi_hourly_zone`, `fact_taxi_air_quality_daily` |
| NYC TLC Taxi Zone Lookup CSV (`taxi_zone_lookup.csv`) | `Files/taxi/taxi_zone_lookup.csv` | *(Validated directly into Gold)* | `dim_taxi_zone` |
| OpenAQ v3 API (`/sensors/{id}/hours`, `/sensors/{id}`, `/locations`, `/sensors/{id}/flags`) | `Files/air_quality/openaq/raw/...`, `Files/air_quality/openaq/metadata/...`, `bronze_openaq_hourly`, `bronze_openaq_sensor`, `bronze_openaq_location`, `bronze_openaq_flags`, `bronze_openaq_ingest_audit` | `silver_air_quality_hourly`, `silver_air_quality_quarantine`, `silver_air_quality_sensor`, `silver_air_quality_flags`, `silver_air_quality_quality_audit` | `dim_air_quality_sensor`, `fact_air_quality_daily_sensor`, `fact_air_quality_daily_citywide`, `fact_air_quality_hourly_citywide`, `fact_taxi_air_quality_daily` |
| World Bank v2 API (`NY.GDP.MKTP.CD`) | `Files/economics/world_bank/raw/...`, `bronze_world_bank_gdp`, `bronze_world_bank_ingest_audit` | `silver_world_bank_gdp`, `silver_world_bank_gdp_quarantine`, `silver_economics_quality_audit` | `dim_country`, `fact_gdp_annual` |
| ECB Data API (`EXR/D.USD.EUR.SP00.A`) | `Files/economics/ecb_fx/raw/...`, `bronze_ecb_fx`, `bronze_ecb_fx_ingest_audit` | `silver_ecb_fx`, `silver_ecb_fx_quarantine`, `silver_economics_quality_audit` | `dim_currency`, `fact_fx_daily`, `vw_taxi_revenue_usd_eur_daily` |
| Quality Audits & Run Validation | N/A | *(Silver audit tables above)* | `etl_run_log`, `vw_audit_taxi_quality`, `vw_audit_air_quality`, `vw_audit_economics`, `vw_etl_run_log` |

Note that `dim_taxi_zone` intentionally bypasses `lh_silver`. `nb_taxi_gold` refreshes the lookup CSV directly into `lh_bronze/Files/taxi/taxi_zone_lookup.csv` (falling back to the cached Bronze file if the external download fails), validates non-null unique `LocationID` values, and appends fallback rows for `-1` or any unmatched pickup IDs seen in `silver_taxi`.

---

## 2. Silver Layer (`lh_silver`)

### NYC Taxi (`nb_taxi_silver`)

All three taxi Silver tables are overwritten one month at a time using Delta `replaceWhere` (`year_month` on `silver_taxi`, `source_year_month` on quarantine and audit) so reprocessing a single month leaves other months intact.

`dbo.silver_taxi` holds one in-scope trip record (`month_start <= tpep_pickup_datetime < month_end`, with non-negative duration or missing dropoff). Every run casts all 20 canonical TLC columns (`VendorID`, `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `passenger_count`, `trip_distance`, `RatecodeID`, `store_and_fwd_flag`, `PULocationID`, `DOLocationID`, `payment_type`, `fare_amount`, `extra`, `mta_tax`, `tip_amount`, `tolls_amount`, `improvement_surcharge`, `total_amount`, `congestion_surcharge`, `Airport_fee`, `cbd_congestion_fee`) to an explicit schema so Parquet drift cannot break downstream reads. It also appends lineage columns (`source_year_month`, `source_file`, SHA-256 `record_hash`, `duplicate_group_size`), derived time/duration columns (`year_month`, `pickup_date`, `pickup_hour`, nullable `trip_duration_min`), and 16 non-null boolean flags (`is_zero_distance`, `is_missing_distance`, `is_negative_distance`, `is_unusual_distance`, `is_missing_dropoff_datetime`, `is_zero_duration`, `is_unusual_duration`, `is_negative_fare`, `is_negative_total`, `is_negative_amount`, `is_missing_fare`, `is_missing_total`, `is_missing_amount`, `is_passenger_missing`, `is_missing_pickup_location`, `is_duplicate_record`) along with `has_any_quality_flag`, `quality_issue_count`, and comma-delimited `quality_flags`.

`dbo.silver_taxi_quarantine` captures records triggered by `is_missing_pickup_datetime`, `is_dropoff_before_pickup`, or `is_out_of_month`. It keeps all source, lineage, and quality columns alongside `quarantine_scope` (`IN_SCOPE_QUARANTINE` or `OUT_OF_MONTH`) and `quarantine_reasons`. Routing out-of-month pickups here prevents a file from one month carrying stray pickups into another month's `replaceWhere` partition while keeping Bronze row reconciliation exact.

`dbo.silver_taxi_quality_audit` records one reconciliation row per `source_year_month` containing `bronze_row_count`, `silver_row_count`, `quarantine_total_count` (split by scope), `reconciliation_diff`, `reconciliation_passed`, per-condition counts, and `legacy_filter_excluded_count` to track how many rows naive pre-filters would have removed.

### OpenAQ Air Quality (`nb_build_silver_air_quality`)

Hourly observations and quarantine rows replace month by month using `replaceWhere` over `[month_start, month_end)` on `local_date` (Eastern Time), while `bronze_openaq_*` ingestion runs across padded UTC bounds so Eastern midnight boundaries are never clipped.

`dbo.silver_air_quality_hourly` and `dbo.silver_air_quality_quarantine` share the `(sensor_id, timestamp_utc)` grain and retain local-time fields (`local_datetime`, `local_date`, `local_hour`, `local_year_month`), `pm25_value`, `unit`, `parameter`, within-hour summary statistics (`within_hour_min`, `within_hour_max`, `within_hour_mean`, `source_hour_coverage_percent`), nullable `has_flags`, tri-state `flag_status` (`CONFIRMED_FLAGGED`, `CONFIRMED_UNFLAGGED`, `UNKNOWN_FLAG_STATUS`), `raw_has_flags`, and interval flag summaries (`active_flag_event_count`, `active_flag_labels`). Only `CONFIRMED_UNFLAGGED` rows with valid units/parameters and non-negative `pm25_value` enter `silver_air_quality_hourly`. Quarantined rows also retain `quality_status` (priority status), `quality_reasons` (all triggered failure reasons preserved together), `quality_issue_count`, and individual rule booleans.

Three supporting Silver tables complete the air-quality layer: `dbo.silver_air_quality_sensor` stores validated metadata for the 8 AirNow monitors at `sensor_id` grain, `dbo.silver_air_quality_flags` tracks maintenance and flag intervals from `/sensors/{id}/flags` overlapping the processed months while preserving intervals outside `MONTHS`, and `dbo.silver_air_quality_quality_audit` verifies `in_scope_bronze_count == written_accepted_count + written_quarantine_count` for each `local_year_month`.

### Economics (`nb_build_silver_economics`)

Because World Bank GDP and daily ECB FX are small reference datasets, their Silver tables overwrite atomically on each run after uniqueness checks and row-count reconciliation.

| Table | Grain | Rules & Quarantine Handling |
| --- | --- | --- |
| `dbo.silver_world_bank_gdp` | `(indicator_id, country_iso3, year)` | Keeps rows where `gdp_usd > 0`. |
| `dbo.silver_world_bank_gdp_quarantine` | `(indicator_id, country_iso3, year)` | Captures `MISSING_VALUE`, `INVALID_KEY`, or `NON_POSITIVE_VALUE`. |
| `dbo.silver_ecb_fx` | `(currency_pair, observation_date)` | Stores `usd_per_eur_rate` and `eur_per_usd_rate = 1.0 / usd_per_eur_rate`. |
| `dbo.silver_ecb_fx_quarantine` | `(currency_pair, observation_date)` | Captures `INVALID_DATE`, `MISSING_RATE`, or `NON_POSITIVE_RATE`. |
| `dbo.silver_economics_quality_audit` | `dataset` | Two rows (`WORLD_BANK_GDP`, `ECB_FX`) confirming reconciliation before Gold runs. |

---

## 3. Gold & Warehouse Star Schema (`lh_gold` / `wh_nyc_analytics`)

`wh_nyc_analytics` exposes 18 SQL views reading directly from `lh_gold` and `lh_silver` via cross-database Three-Part Names, keeping queries aligned with the latest Gold run without duplicating Delta storage.

### Dimensions

| Table / View | Grain | Key | Notes |
| --- | --- | --- | --- |
| `dim_date` | Calendar day | `date_key` (`YYYYMMDD` int) | Spans min..max bounds across all processed months (merged with any existing bounds). |
| `dim_taxi_zone` | TLC Zone | `location_id` (`int`) | Official TLC lookup zones plus explicit `-1` (`Missing Pickup Location`) and `is_known_zone = false` fallback rows for unmatched IDs. |
| `dim_air_quality_sensor` | OpenAQ sensor | `sensor_id` (`bigint`) | Metadata and coordinates for the 8 selected AirNow monitors. |
| `dim_currency` | Currency | `currency_code` (`USD`, `EUR`) | Reference dimension marking `EUR` as the ECB base currency. |
| `dim_country` | Country / region | `country_iso3` (`USA`, `EMU`) | Built from valid and quarantined GDP entities so metadata stays complete, linked to `primary_currency_code`. |

### Facts

| Table / View | Grain | Key | Notes |
| --- | --- | --- | --- |
| `fact_taxi_daily` | Calendar day | `date_key` | Month-complete calendar grid (`has_observed_trips` distinguishes zero-trip dates from missing data). Reports trip volume, gross nonnegative vs. signed net fares/totals, distance averages, quality counts, and `payment_type` slices. |
| `fact_taxi_hourly_zone` | Day + hour + zone | `(date_key, pickup_hour, PULocationID)` | Maps missing pickup zones to `-1` and reconciles trip counts, revenue, and distance flags directly back to `fact_taxi_daily`. |
| `fact_air_quality_daily_sensor` | Sensor + local date | `(sensor_id, local_date)` | Complete `8 × calendar days` grid with DST-aware `expected_hour_count` (`23`, `24`, or `25`), accepted vs. quarantined hour splits, and `hour_coverage_pct`. |
| `fact_air_quality_daily_citywide` | Local date | `local_date` | Network mean/median PM2.5 (`NULL` if `< 4` eligible sensors meet `>= 75%` valid unflagged coverage). |
| `fact_air_quality_hourly_citywide` | Local date + local hour | `(local_date, local_hour)` | Complete DST-aware hourly grid (`expected_utc_hours` distinguishes DST spring-forward/fall-back slots). Sets hourly network PM2.5 to `NULL` if `< 4` sensors report valid readings. |
| `fact_taxi_air_quality_daily` | Calendar day | `date_key` | Month-scoped left join of `fact_taxi_daily` with `fact_air_quality_daily_citywide`, preserving all taxi dates even when citywide PM2.5 is `NULL`. |
| `fact_fx_daily` | Day + currency pair | `(date_key, currency_pair)` | Complete calendar grid using `ECB_LAST_BUSINESS_DAY_CARRY_FORWARD` (`currency_pair = 'EUR/USD'`), retaining `rate_observation_date`, `is_carry_forward_rate`, and `days_since_rate_observation`. |
| `fact_gdp_annual` | Country + year | `(country_iso3, year)` | Annual numeric GDP (`NY.GDP.MKTP.CD` in USD). Numeric GDP lives in a fact table rather than a country dimension. |

### Analytical Views & Operational Tables

| Table / View | Grain | Key | Notes |
| --- | --- | --- | --- |
| `vw_taxi_revenue_usd_eur_daily` | Calendar day | `date_key` | Multiplies `fact_taxi_daily` USD totals by `fact_fx_daily.eur_per_usd_rate` (`EUR/USD`) and exposes carry-forward metadata. |
| `vw_audit_taxi_quality` | `source_year_month` | `source_year_month` | Warehouse view over `lh_silver.dbo.silver_taxi_quality_audit`. |
| `vw_audit_air_quality` | `local_year_month` | `local_year_month` | Warehouse view over `lh_silver.dbo.silver_air_quality_quality_audit`. |
| `vw_audit_economics` | `dataset` | `dataset` | Warehouse view over `lh_silver.dbo.silver_economics_quality_audit`. |
| `etl_run_log` / `vw_etl_run_log` | Run + source + month | `(run_id, source_name, selected_month)` | Written by `nb_validate_pipeline_run` at the end of every master pipeline run with read/write counts, timestamps, status, and error details. |
