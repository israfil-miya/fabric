# Governance, Data Quality Rules & Analytical Assumptions

Integrating four real-world datasets required deciding upfront which irregular records reflect genuine domain behavior versus corrupted inputs, and making every decision auditable in Delta tables rather than hiding it inside filters.

## 1. NYC Taxi Quality & Revenue Accounting

Raw TLC Yellow Taxi files contain records that look broken against strict transaction-log assumptions: zero-distance metered rides, negative totals, 12-hour durations, negative dropoff durations around DST changes, and pickup timestamps belonging to adjacent months. Silently dropping those rows alters trip counts and financial totals without leaving a trail.

### What Gets Quarantined vs. Flagged

`nb_taxi_silver` partitions Bronze Parquet records strictly between `silver_taxi` and `silver_taxi_quarantine` so `silver_row_count + quarantine_total_count == bronze_row_count` holds on every run.

Quarantine captures only the three conditions that prevent a record from belonging safely to a monthly Silver slice: missing `tpep_pickup_datetime`, `tpep_dropoff_datetime < tpep_pickup_datetime` (such as `1,435` clock/DST timeline anomalies in `2025-11`), or valid pickups falling outside the Parquet file's expected `year_month` (`OUT_OF_MONTH`). Isolating out-of-month pickups keeps one month's file from overwriting another month's `replaceWhere` partition while preserving full Bronze reconciliation.

Valid-timeline in-month anomalies stay in `silver_taxi` with explicit boolean flags and a comma-joined `quality_flags` summary:

- **Distance edge cases:** Zero-distance rides remain `0.0` (never rewritten to `NULL`) and flag `is_zero_distance`, as they often represent valid cancellations, flat-rate adjustments, or metered idle time. Missing (`NULL`/`NaN`) and negative distances flag `is_missing_distance` or `is_negative_distance`, stay in `trip_count`, and are excluded from distance averages. Rides exceeding 100 miles flag `is_unusual_distance` instead of being deleted.
- **Duration edge cases:** Missing dropoff timestamps stay in Silver (`is_missing_dropoff_datetime`) because a valid pickup timestamp still supports date and hour attribution (`trip_duration_min` remains `NULL`). Zero durations (`is_zero_duration`) and trips exceeding 8 hours (`is_unusual_duration`) stay preserved rather than cropped.
- **Missing keys and exact duplicates:** Null passenger counts (`is_passenger_missing`) and null `PULocationID` values (`is_missing_pickup_location`, mapped to `-1` in Gold) remain counted. Exact duplicates across all 20 source columns are tagged via a SHA-256 `record_hash` (`is_duplicate_record`, `duplicate_group_size`) without automatic deduplication.

### Gross vs. Signed Net Revenue and Flex Fares

Negative `fare_amount` and `total_amount` values are legitimate accounting entries in TLC data. Disputes, voided trips, and `payment_type = 0` (Flex Fare) adjustments frequently carry negative values, and `2025-11` contains an especially large spike in negative adjustment records compared with `2025-12` onward.

To prevent ambiguity between trip revenue collected and net ledger totals after adjustments, `fact_taxi_daily` applies one shared eligibility rule (`~is_missing_amount & ~is_negative_amount`) across `revenue_trip_count`, `total_fare_usd`, and `total_amount_usd`. That ensures average revenue per eligible trip uses matching numerators and denominators. Alongside those columns, Gold exposes measure-specific nonnegative totals (`gross_nonnegative_fare_usd`, `gross_nonnegative_total_amount_usd`), signed net totals (`net_fare_usd`, `net_total_amount_usd`, plus `net_fare_flex_usd` and `net_total_amount_flex_usd`), and explicit counts for all TLC `payment_type` codes (`0` Flex Fare through `6` Voided trip).

---

## 2. OpenAQ Air Quality Design & Limitations

Air-quality monitoring data introduces a separate set of failure modes, including API maintenance flags, mixed sensor tiers, UTC-to-local boundary shifts, and station downtime.

OpenAQ's `hasFlags` attribute is mapped to three states (`CONFIRMED_FLAGGED` for `True`, `CONFIRMED_UNFLAGGED` for `False`, and `UNKNOWN_FLAG_STATUS` for `NULL`) rather than coercing missing flag metadata to `False`. Only `CONFIRMED_UNFLAGGED` observations with `parameter = 'pm25'`, `unit = 'µg/m³'`, and non-negative `pm25_value` enter `silver_air_quality_hourly`. When a reading fails multiple checks (such as `MISSING_VALUE,FLAGGED`), `silver_air_quality_quarantine` preserves every triggered reason in `quality_reasons` alongside the primary `quality_status`. Interval flag events from `/sensors/{id}/flags` are stored in `silver_air_quality_flags` and joined onto hourly records as counts and sorted label lists without collapsing distinct overlapping events.

Sensor selection is kept explicit and inspectable. `nb_ingest_openaq` targets eight validated AirNow regulatory monitors (`673, 1097, 1102, 1103, 1152, 1128, 1145, 1146`), while `nb_build_silver_air_quality` keeps `INSPECT_CANDIDATE_SENSORS = True` by default so Bronze candidates can still be compared on `is_monitor`, coordinates, instruments, and flag volume before expanding the network.

Citywide aggregation respects Daylight Saving Time and minimum station coverage. `fact_air_quality_daily_sensor` computes `expected_hour_count` dynamically in `America/New_York` (`25` on `2025-11-02`, `23` on `2026-03-08`, `24` otherwise), and `fact_air_quality_hourly_citywide` aggregates per `(local_date, local_hour, sensor_id)` first so a fall-back double hour gives each sensor one vote. A sensor-day contributes to daily citywide metrics only when `hour_coverage_pct >= 75.0`, and a citywide day requires `eligible_sensor_count >= 4`. Days below that threshold (such as 19 low-coverage days in early November 2025) keep `network_mean_pm25` and `network_median_pm25` as `NULL` rather than filling `0`. Because eight fixed AirNow monitors capture a monitoring-network average rather than neighborhood-level air quality, no zone-level causal link is claimed between local taxi density and PM2.5.

---

## 3. Economics & Currency Conversion (ECB + World Bank)

ECB series `EXR/D.USD.EUR.SP00.A` publishes how many US dollars equal one euro (`base_currency = 'EUR'`, `quote_currency = 'USD'`). Silver stores both `usd_per_eur_rate` and its exact reciprocal `eur_per_usd_rate = 1.0 / usd_per_eur_rate`, so converting daily USD taxi revenue to EUR multiplies cleanly by `eur_per_usd_rate`.

Because ECB publishes rates only on European business days while taxi ridership runs year-round, `nb_ingest_ecb_fx` buffers API requests back 14 calendar days before the earliest selected month. Even when a month starts on a weekend or holiday, `fact_fx_daily` finds a valid prior rate and carries it forward under `ECB_LAST_BUSINESS_DAY_CARRY_FORWARD`, retaining `rate_observation_date`, `is_carry_forward_rate`, and `days_since_rate_observation` for downstream audits. World Bank indicator `NY.GDP.MKTP.CD` is modeled separately as annual numeric fact `fact_gdp_annual` (`USA` and Eurozone aggregate `EMU`), with unpublished or null years routed to `silver_world_bank_gdp_quarantine`.

---

## 4. Operations, Secrets and Monitoring

To avoid provisioning Azure Key Vault solely for one free OpenAQ token, `nb_ingest_openaq` reads the API key from `lh_bronze/Files/_secrets/openaq.json` (git-ignored and outside notebook parameter snapshots), while retaining optional `KEY_VAULT_URL` lookup via `notebookutils.credentials.getSecret` if a Key Vault is added later.

Upstream quality checks act as publishing gates. Every Gold notebook refuses to run if its Silver audit table (`silver_*_quality_audit`) is missing or has `reconciliation_passed = False`. At the end of `pl_nyc_analytics_master`, `nb_validate_pipeline_run` cross-checks Bronze audits, Silver audits, Gold row counts, and key uniqueness across all sources before appending run status rows to `lh_gold.dbo.etl_run_log`.

Automated Fabric pipeline schedules, workspace role separation beyond notebook execution, and downstream Power BI semantic models (including Row-Level Security if scoped by borough or country) are not configured in this repository yet. Right now, `wh_nyc_analytics` serves the read-only T-SQL view layer and `nb_analytics_insights` provides visual exploration.
