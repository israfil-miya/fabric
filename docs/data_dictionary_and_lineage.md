# Data Dictionary & Source-to-Target Lineage

## 1. Source-to-Target Mapping

| Source | Bronze Storage / Table (`lh_bronze`) | Silver Table (`lh_silver`) | Gold / Warehouse Table (`lh_gold` / `wh_nyc_analytics`) |
| --- | --- | --- | --- |
| NYC TLC Yellow Taxi Parquet (`yellow_tripdata_YYYY-MM.parquet`) | `Files/taxi/yellow_tripdata_YYYY-MM.parquet` | `silver_taxi`, `silver_taxi_quarantine`, `silver_taxi_quality_audit` | `dim_date`, `fact_taxi_daily`, `fact_taxi_hourly_zone`, `fact_taxi_air_quality_daily` |
| NYC TLC Taxi Zone Lookup CSV (`taxi_zone_lookup.csv`) | `Files/taxi/taxi_zone_lookup.csv` | *(Validated directly into Gold)* | `dim_taxi_zone` |
| OpenAQ v3 API (`/sensors/{id}/hours`, `/sensors/{id}`, `/locations`, `/sensors/{id}/flags`) | `Files/air_quality/openaq/raw/...` + `bronze_openaq_hourly`, `bronze_openaq_sensor`, `bronze_openaq_location`, `bronze_openaq_flags`, `bronze_openaq_ingest_audit` | `silver_air_quality_hourly`, `silver_air_quality_quarantine`, `silver_air_quality_sensor`, `silver_air_quality_flags`, `silver_air_quality_quality_audit` | `dim_air_quality_sensor`, `fact_air_quality_daily_sensor`, `fact_air_quality_daily_citywide`, `fact_air_quality_hourly_citywide`, `fact_taxi_air_quality_daily` |
| World Bank v2 API (`NY.GDP.MKTP.CD`) | `Files/economics/world_bank/raw/...` + `bronze_world_bank_gdp`, `bronze_world_bank_ingest_audit` | `silver_world_bank_gdp`, `silver_world_bank_gdp_quarantine`, `silver_economics_quality_audit` | `dim_country`, `fact_gdp_annual` |
| ECB Data API (`EXR/D.USD.EUR.SP00.A`) | `Files/economics/ecb_fx/raw/...` + `bronze_ecb_fx`, `bronze_ecb_fx_ingest_audit` | `silver_ecb_fx`, `silver_ecb_fx_quarantine`, `silver_economics_quality_audit` | `dim_currency`, `fact_fx_daily`, `vw_taxi_revenue_usd_eur_daily` |
| Pipeline Execution & Validation | — | — | `etl_run_log` |

---

## 2. Silver Layer Dictionary (`lh_silver`)

### `dbo.silver_taxi` (partitioned/replaced by `year_month`)
- **Grain:** One in-scope Yellow Taxi trip record (`month_start <= tpep_pickup_datetime < month_end`, `tpep_dropoff_datetime >= tpep_pickup_datetime` or `tpep_dropoff_datetime IS NULL`).
- **Source & Lineage Columns:** All 20 canonical TLC columns (`VendorID`, `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `passenger_count`, `trip_distance`, `RatecodeID`, `store_and_fwd_flag`, `PULocationID`, `DOLocationID`, `payment_type`, `fare_amount`, `extra`, `mta_tax`, `tip_amount`, `tolls_amount`, `improvement_surcharge`, `total_amount`, `congestion_surcharge`, `Airport_fee`, `cbd_congestion_fee`), plus `source_year_month`, `source_file`, `record_hash` (SHA-256), `duplicate_group_size`.
- **Derived Columns:** `year_month`, `pickup_date`, `pickup_hour`, `trip_duration_min` (`NULL` when dropoff missing).
- **Quality Flags (non-null booleans):** `is_zero_distance`, `is_missing_distance`, `is_negative_distance`, `is_unusual_distance` (`> 100`), `is_missing_dropoff_datetime`, `is_zero_duration`, `is_unusual_duration` (`> 480` min), `is_negative_fare`, `is_negative_total`, `is_negative_amount`, `is_missing_fare`, `is_missing_total`, `is_missing_amount`, `is_passenger_missing`, `is_missing_pickup_location`, `is_duplicate_record`, `has_any_quality_flag`, `quality_issue_count`, `quality_flags`.

### `dbo.silver_taxi_quarantine` (replaced by `source_year_month`)
- **Grain:** One quarantined Yellow Taxi trip record.
- **Trigger Conditions:** `is_missing_pickup_datetime`, `is_dropoff_before_pickup`, or `is_out_of_month`.
- **Columns:** All 20 source columns + lineage + all quality flags + `quarantine_scope` (`IN_SCOPE_QUARANTINE` or `OUT_OF_MONTH`) + `quarantine_reasons`.

### `dbo.silver_taxi_quality_audit` (replaced by `source_year_month`)
- **Grain:** One row per `source_year_month`.
- **Key Columns:** `bronze_row_count`, `silver_row_count`, `quarantine_total_count`, `quarantine_in_scope_count`, `quarantine_out_of_month_count`, `reconciliation_diff`, `reconciliation_passed`, `legacy_filter_excluded_count`, `legacy_filter_excluded_pct`, plus per-condition counts.

### `dbo.silver_air_quality_hourly` & `dbo.silver_air_quality_quarantine` (replaced by `local_date` month range)
- **Grain:** One observation per `(sensor_id, timestamp_utc)`.
- **Key Columns:** `sensor_id`, `location_id`, `location_name`, `timestamp_utc`, `timestamp_end_utc`, `local_datetime`, `local_date`, `local_hour`, `local_year_month`, `pm25_value`, `unit`, `parameter`, `has_flags` (nullable boolean), `flag_status` (`CONFIRMED_FLAGGED` / `CONFIRMED_UNFLAGGED` / `UNKNOWN_FLAG_STATUS`), `raw_has_flags`, `active_flag_event_count`, `active_flag_labels`, `within_hour_min`, `within_hour_max`, `within_hour_mean`, `source_hour_coverage_percent`. Quarantine also retains `quality_status`, `quality_reasons`, `quality_issue_count`, and per-rule booleans.

### `dbo.silver_world_bank_gdp` & `dbo.silver_ecb_fx` (+ quarantine tables & `silver_economics_quality_audit`)
- `silver_world_bank_gdp`: `(indicator_id, country_iso3, year)` with valid `gdp_usd > 0`; missing/unpublished years preserved in `silver_world_bank_gdp_quarantine`.
- `silver_ecb_fx`: `(currency_pair, observation_date)` with `usd_per_eur_rate > 0` and `eur_per_usd_rate = 1.0 / usd_per_eur_rate`; missing/invalid rows preserved in `silver_ecb_fx_quarantine`.

---

## 3. Gold & Warehouse Star Schema Dictionary (`lh_gold` / `wh_nyc_analytics`)

| Table / View | Grain | Primary / Composite Key | Notes |
| --- | --- | --- | --- |
| `dim_date` | Calendar day | `date_key` (`YYYYMMDD` int) | Covers full min..max span of processed months |
| `dim_taxi_zone` | TLC Zone | `location_id` (`int`) | Includes `-1` (`Missing Pickup Location`) and any unmatched IDs (`is_known_zone = false`) |
| `dim_air_quality_sensor` | OpenAQ sensor | `sensor_id` (`bigint`) | 8 selected AirNow monitors |
| `dim_currency` | Currency | `currency_code` (`USD`, `EUR`) | Reference currency dimension |
| `dim_country` | Country / region | `country_iso3` (`USA`, `EMU`) | Links to `primary_currency_code` |
| `fact_taxi_daily` | Calendar day | `date_key` | Complete calendar grid per processed month (`has_observed_trips` distinguishes zero-trip vs. missing file) |
| `fact_taxi_hourly_zone` | Day + hour + zone | `(date_key, pickup_hour, PULocationID)` | Maps missing `PULocationID` to `-1`; reconciles to `fact_taxi_daily` |
| `fact_air_quality_daily_sensor` | Sensor + local date | `(sensor_id, local_date)` | Complete `8 × days` grid; DST-aware `expected_hour_count` (`23`/`24`/`25`) |
| `fact_air_quality_daily_citywide` | Local date | `local_date` | Network mean/median PM2.5 (`NULL` if `< 4` eligible sensors at `>= 75%` coverage) |
| `fact_air_quality_hourly_citywide` | Local date + local hour | `(local_date, local_hour)` | Complete DST-aware hourly grid; `hourly_network_mean_pm25` is `NULL` when `< 4` sensors report valid unflagged PM2.5 |
| `fact_taxi_air_quality_daily` | Calendar day | `date_key` | Left join of `fact_taxi_daily` + `fact_air_quality_daily_citywide` |
| `fact_fx_daily` | Day + currency pair | `(date_key, currency_pair)` | Explicit `ECB_LAST_BUSINESS_DAY_CARRY_FORWARD` with `rate_observation_date` |
| `fact_gdp_annual` | Country + year | `(country_iso3, year)` | Annual numeric GDP (`NY.GDP.MKTP.CD`) |
| `vw_taxi_revenue_usd_eur_daily` | Calendar day | `date_key` | Warehouse view multiplying USD measures by `eur_per_usd_rate` |
| `etl_run_log` | Run + source + month | `(run_id, source_name, selected_month)` | Records read/written counts, timestamps, status, and errors |
