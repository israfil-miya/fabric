# Governance, Data Quality Rules & Analytical Assumptions

Building a pipeline across four real-world datasets meant deciding upfront which weird rows represent genuine domain behavior versus corrupted data, and making every decision auditable in Delta tables instead of burying it inside filters.

## 1. NYC Taxi Quality & Revenue Accounting

Raw TLC Yellow Taxi files contain records that look broken against strict transaction-log assumptions: zero-distance metered rides, negative totals, 12-hour durations, negative dropoff durations around DST changes, and pickup timestamps belonging to adjacent months. Silently dropping those rows changes trip counts and totals without leaving a trail.

### What Gets Quarantined vs. Flagged

`nb_taxi_silver` splits Bronze Parquet records strictly into `silver_taxi` and `silver_taxi_quarantine` so `silver_row_count + quarantine_total_count == bronze_row_count` on every run:

- **Quarantined (`silver_taxi_quarantine`):** Only three conditions prevent a trip from sitting inside a monthly Silver partition cleanly: missing `tpep_pickup_datetime`, `tpep_dropoff_datetime < tpep_pickup_datetime` (for example, `1,435` clock/DST timeline anomalies in `2025-11`), or valid pickups falling outside the Parquet file's expected `year_month` (`OUT_OF_MONTH`). Quarantining out-of-month rows keeps one month's file from overlapping another month's `replaceWhere` slice while preserving full source reconciliation.
- **Kept and flagged (`silver_taxi`):** Valid-timeline in-month anomalies stay in Silver with explicit boolean flags and a comma-joined `quality_flags` summary:
  - *Distance edge cases:* Zero-distance rides stay `0.0` (never rewritten to `NULL`) and flag `is_zero_distance`—these often reflect valid cancellations, flat-rate adjustments, or metered idle time. Missing (`NULL`/`NaN`) and negative distances flag `is_missing_distance` / `is_negative_distance`, stay in `trip_count`, and are excluded from distance averages. Rides over 100 miles flag `is_unusual_distance` instead of getting deleted.
  - *Duration edge cases:* Missing dropoff timestamps stay in Silver (`is_missing_dropoff_datetime`) because a valid pickup timestamp still supports date/hour attribution (`trip_duration_min` stays `NULL`). Durations matching pickup (`is_zero_duration`) or exceeding 8 hours (`trip_duration_min > 480`, flagged `is_unusual_duration`) stay preserved rather than cropped.
  - *Missing dimensions & exact duplicates:* Null passenger counts (`is_passenger_missing`) and null `PULocationID` values (`is_missing_pickup_location`, mapped to `-1` in Gold) remain counted. Exact row duplicates across all 20 source columns are tagged via a SHA-256 `record_hash` (`is_duplicate_record`, `duplicate_group_size`) without deduplicating them automatically.

### Gross vs. Signed Net Revenue and Flex Fares

Negative `fare_amount` and `total_amount` values are real accounting entries in TLC data—disputes, voided trips, and `payment_type = 0` (Flex Fare) adjustments frequently carry negative values, and `2025-11` has an especially heavy spike in negative adjustment records compared with `2025-12` onward.

To prevent ambiguity between "trip revenue collected" and "net ledger total after adjustments":

- `revenue_trip_count`, `total_fare_usd`, and `total_amount_usd` share the same eligibility filter (`~is_missing_amount & ~is_negative_amount`), ensuring average revenue per eligible trip uses matching numerators and denominators.
- `fact_taxi_daily` also writes measure-specific nonnegative columns (`gross_nonnegative_fare_usd`, `gross_nonnegative_total_amount_usd`) alongside signed net columns (`net_fare_usd`, `net_total_amount_usd`, plus `net_fare_flex_usd` and `net_total_amount_flex_usd`) so negative adjustments remain transparent rather than hidden.
- Trip counts for all official TLC `payment_type` codes (`0` Flex Fare through `6` Voided trip) are stored explicitly.

---

## 2. OpenAQ Air Quality Design & Limitations

Air-quality monitoring data brings a different set of failure modes: API maintenance flags, mixed sensor tiers, UTC-to-local boundary shifts, and station downtime.

- **Tri-state flag handling (`flag_status`):** OpenAQ's `hasFlags` field is treated as three states—`CONFIRMED_FLAGGED` (`True`), `CONFIRMED_UNFLAGGED` (`False`), and `UNKNOWN_FLAG_STATUS` (`NULL`). Missing flag metadata is never coerced to `False`. Only `CONFIRMED_UNFLAGGED` observations with `parameter = 'pm25'`, `unit = 'µg/m³'`, and non-negative `pm25_value` enter `silver_air_quality_hourly`.
- **Multi-reason quarantine & interval flags:** If an hourly reading is both missing a value and flagged (`MISSING_VALUE,FLAGGED`), `silver_air_quality_quarantine` preserves all failing reasons in `quality_reasons` alongside the primary `quality_status`. Interval flag events fetched from `/sensors/{id}/flags` are stored in `silver_air_quality_flags` and joined onto hourly records as counts and sorted label lists without collapsing distinct active flags.
- **Selecting 8 AirNow reference monitors:** `nb_ingest_openaq` targets eight validated AirNow regulatory monitors (`673, 1097, 1102, 1103, 1152, 1128, 1145, 1146`). `nb_build_silver_air_quality` leaves `INSPECT_CANDIDATE_SENSORS = True` by default so Bronze candidate tables can still be compared on `is_monitor`, coordinates, instrument, and flag volume before expanding the network.
- **DST-aware grids and minimum daily eligibility:** Because NYC observes Daylight Saving Time, `fact_air_quality_daily_sensor` computes `expected_hour_count` dynamically per `local_date` (`America/New_York`: `25` on `2025-11-02`, `23` on `2026-03-08`, `24` otherwise), and `fact_air_quality_hourly_citywide` aggregates per `(local_date, local_hour, sensor_id)` first so a fall-back double hour gives each sensor one vote. A sensor-day is eligible toward citywide metrics only when `hour_coverage_pct >= 75.0`, and a citywide day requires `eligible_sensor_count >= 4`—otherwise `network_mean_pm25` and `network_median_pm25` stay `NULL` (leaving 19 low-coverage days in early November 2025 blank rather than biased).
- **Spatial limitation:** Eight fixed AirNow monitors measure a citywide monitoring network, not neighborhood-level exposure inside individual taxi zones. Nothing in this project claims zone-level causal impact between taxi density and localized PM2.5.

---

## 3. Economics & Currency Conversion (ECB + World Bank)

- **Quote direction (`EUR/USD`):** ECB series `EXR/D.USD.EUR.SP00.A` publishes how many US dollars equal one euro (`base_currency = 'EUR'`, `quote_currency = 'USD'`). Silver stores both `usd_per_eur_rate` and its exact reciprocal `eur_per_usd_rate = 1.0 / usd_per_eur_rate`, so converting daily USD taxi revenue to EUR multiplies cleanly by `eur_per_usd_rate`.
- **Weekend and holiday carry-forward:** ECB only publishes rates on European business days, whereas taxi activity runs 365 days a year. `nb_ingest_ecb_fx` buffers API queries back 14 calendar days before the earliest selected month so that even if a month starts on a weekend or holiday, `fact_fx_daily` always finds a prior valid rate. Carried-forward days carry policy `ECB_LAST_BUSINESS_DAY_CARRY_FORWARD` along with `rate_observation_date`, `is_carry_forward_rate`, and `days_since_rate_observation`.
- **GDP modeling:** World Bank indicator `NY.GDP.MKTP.CD` is modeled as annual numeric fact `fact_gdp_annual` (`USA` and Eurozone aggregate `EMU`), with unpublished or null years routed to `silver_world_bank_gdp_quarantine`.

---

## 4. Operations, Secrets & Monitoring

- **API credentials without Key Vault overhead:** To avoid provisioning Azure Key Vault strictly for one free OpenAQ API key, `nb_ingest_openaq` reads the token from `lh_bronze/Files/_secrets/openaq.json` (git-ignored and outside notebook parameter snapshots), while retaining optional `KEY_VAULT_URL` lookup via `notebookutils.credentials.getSecret` if a Key Vault is attached later.
- **Pre-flight and post-run gates:** Every Gold notebook refuses to publish if its upstream Silver audit table (`silver_*_quality_audit`) is missing or has `reconciliation_passed = False`. At the end of `pl_nyc_analytics_master`, `nb_validate_pipeline_run` cross-checks Bronze audits, Silver audits, Gold counts, and key uniqueness across all sources and appends one row per source/month to `lh_gold.dbo.etl_run_log`.
- **What is not configured yet:** Pipeline scheduling in Fabric and a Power BI semantic model / report layer (including workspace RBAC separation or Power BI Row-Level Security if analysts are scoped by borough or country) are left for downstream deployment. Right now, `wh_nyc_analytics` provides the read-only T-SQL view layer and `nb_analytics_insights` provides the visual analysis.
