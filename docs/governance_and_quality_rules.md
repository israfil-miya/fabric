# Governance, Data Quality Rules & Analytical Assumptions

## 1. NYC Taxi Data Quality & Accounting Rules

### Record Preservation & Quarantine Policy
| Condition | Treatment | Rationale |
| --- | --- | --- |
| `tpep_pickup_datetime IS NULL` | Quarantine (`silver_taxi_quarantine`) | Cannot be assigned reliably to a pickup date or month. |
| `tpep_dropoff_datetime < tpep_pickup_datetime` | Quarantine (`silver_taxi_quarantine`) | Invalid timeline (e.g., clock/DST anomalies such as the 1,435 rows in `2025-11`). |
| Valid pickup outside file's `year_month` | Quarantine (`OUT_OF_MONTH` scope) | Preserved for full Bronze reconciliation without polluting another month's `replaceWhere` slice. |
| `trip_distance == 0` | Keep in `silver_taxi`; flag `is_zero_distance` | Kept as `0.0` (not `NULL`); included in `trip_count`, revenue, and `avg_distance_miles`, with `zero_distance_trips` and `zero_distance_pct` tracked separately. |
| `trip_distance IS NULL` / `NaN` | Keep in `silver_taxi`; flag `is_missing_distance` | Included in `trip_count` and revenue; excluded only from distance averages. |
| `trip_distance < 0` | Keep in `silver_taxi`; flag `is_negative_distance` | Preserved for audit; excluded from `avg_distance_miles` and counted in `negative_distance_trips`. |
| `trip_distance > 100` | Keep in `silver_taxi`; flag `is_unusual_distance` | Preserved rather than silently dropped (`915` rows across Nov 2025–Mar 2026). |
| `trip_duration_min > 480` (8 hours) | Keep in `silver_taxi`; flag `is_unusual_duration` | Preserved rather than silently dropped (`5,315` rows across Nov 2025–Mar 2026). |
| `tpep_dropoff_datetime IS NULL` | Keep in `silver_taxi`; flag `is_missing_dropoff_datetime` | Valid pickup time allows date/hour attribution; `trip_duration_min` remains `NULL`. |
| `fare_amount < 0` or `total_amount < 0` | Keep in `silver_taxi`; flag `is_negative_fare` / `is_negative_total` | Reported separately via gross nonnegative (`total_fare_usd`, `total_amount_usd`) and signed net (`net_fare_usd`, `net_total_amount_usd`) totals, and inspected by `payment_type`. |
| `fare_amount IS NULL` or `total_amount IS NULL` | Keep in `silver_taxi`; flag `is_missing_amount` | Excluded from revenue sums; counted in `missing_amount_trips`. |
| Exact duplicate rows | Flag `is_duplicate_record` + `duplicate_group_size` | Identified via SHA-256 `record_hash` without silent deduplication. |

### Revenue & Payment Type Definitions (TLC)
- **`revenue_trip_count`**, **`total_fare_usd`**, and **`total_amount_usd`** share the identical eligibility rule: `~is_missing_amount & ~is_negative_amount`.
- Separate measure-specific nonnegative columns (`gross_nonnegative_fare_usd`, `gross_nonnegative_total_amount_usd`) and signed net columns (`net_fare_usd`, `net_total_amount_usd`) allow exact reconciliation of negative adjustments (disputes, voided trips, and `payment_type = 0` Flex Fare adjustments).
- TLC `payment_type` slices tracked explicitly in `fact_taxi_daily`: `0` (Flex Fare), `1` (Credit card), `2` (Cash), `3` (No charge), `4` (Dispute), `5` (Unknown), `6` (Voided trip).

---

## 2. OpenAQ Air Quality Rules & Limitations

1. **Tri-State Flag Handling:** `coverage.flagInfo.hasFlags` (or top-level `flagInfo.hasFlags`) is mapped to `CONFIRMED_FLAGGED` (`True`), `CONFIRMED_UNFLAGGED` (`False`), or `UNKNOWN_FLAG_STATUS` (`NULL`). Missing flag info is **never** converted to `false`; only `CONFIRMED_UNFLAGGED` records enter `silver_air_quality_hourly`.
2. **Multi-Reason Preservation:** Quarantined observations retain all applicable failure reasons in `quality_reasons` (e.g., `65` records in the 5-month baseline carry `"MISSING_VALUE,FLAGGED"`). Active interval flag events from `/sensors/{id}/flags` are preserved in `silver_air_quality_flags` and summarized onto hourly records without collapsing multiple overlapping events.
3. **8 Selected Sensors vs. 11 Candidates:** `nb_build_silver_air_quality` keeps the 8 validated AirNow reference monitors (`673, 1097, 1102, 1103, 1152, 1128, 1145, 1146`) and exposes `INSPECT_CANDIDATE_SENSORS = True` to compare all Bronze candidates by provider, `is_monitor`, coordinates, instruments, completeness, and flag counts before any expansion decision.
4. **Daily Network Eligibility:** A sensor-day requires `hour_coverage_pct >= 75.0` against DST-aware `expected_hour_count` (`23`, `24`, or `25` hours in `America/New_York`). A citywide day requires `eligible_sensor_count >= 4`; otherwise `network_mean_pm25` and `network_median_pm25` remain `NULL`.
5. **Spatial Coverage Limitation:** The 8 selected monitors represent a monitoring-network estimate, not comprehensive NYC-wide or taxi-zone-level air quality. No causal claim is made between zone-hour taxi density and zone-level PM2.5.

---

## 3. Economic Conversion Policy (ECB & World Bank)

- **Quote Direction:** ECB `EXR/D.USD.EUR.SP00.A` publishes `USD per 1 EUR` (`base_currency = 'EUR'`, `quote_currency = 'USD'`). Both `usd_per_eur_rate` and `eur_per_usd_rate = 1.0 / usd_per_eur_rate` are stored explicitly.
- **Weekend & Holiday Policy:** On non-business days when ECB publishes no rate, `fact_fx_daily` carries forward the most recent prior business-day rate (`ECB_LAST_BUSINESS_DAY_CARRY_FORWARD`) using a 14-day Bronze lookback buffer, retaining `rate_observation_date`, `is_carry_forward_rate`, and `days_since_rate_observation`.
- **GDP Modeling:** World Bank `NY.GDP.MKTP.CD` is modeled as annual numeric fact `fact_gdp_annual` (`country_iso3 + year`), with unpublished/null years quarantined in `silver_world_bank_gdp_quarantine`.

---

## 4. Operations, Security & Governance

- **Secret Management ($0 Azure Cost Pattern):** `nb_ingest_openaq` reads the OpenAQ API key from OneLake (`Files/_secrets/openaq.json` or `Files/openaq.json` in `lh_bronze`), keeping credentials out of Git and out of pipeline/notebook parameter snapshots. Optional `KEY_VAULT_URL` support via `notebookutils.credentials.getSecret` remains available if an Azure Key Vault is provisioned later.
- **Monitoring & Alerting:** Every `pl_nyc_analytics_master` run executes `nb_validate_pipeline_run`, writing per-source/month reconciliation status to `lh_gold.dbo.etl_run_log`. Upstream ingestion, Silver, or Gold failures halt downstream publishing immediately.
- **Scheduled Refresh:** Schedule `pl_nyc_analytics_master` monthly (or daily/weekly in discovery mode) via Fabric Pipeline **Schedule** settings, followed by semantic model refresh on `wh_nyc_analytics`.
- **Data Lineage & Access Control:** Fabric Workspace **Lineage View** (and Microsoft Purview if enabled at tenant level) tracks end-to-end lineage from `lh_bronze` → `lh_silver` → `lh_gold` → `wh_nyc_analytics` → Power BI. Restrict `lh_bronze` write access to pipeline/engineering roles and grant analysts read access via `wh_nyc_analytics` (adding Power BI Row-Level Security on `dim_taxi_zone[borough]` or `dim_country` if department scoping is required).
