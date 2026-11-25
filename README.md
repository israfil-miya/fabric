# NYC Urban Mobility, Air Quality & Macroeconomic Analytics Platform (Microsoft Fabric)

A **month-driven, zero-silent-drop Microsoft Fabric analytics platform** integrating **NYC TLC Yellow Taxi trips**, **OpenAQ v3 PM2.5 air-quality monitoring**, **World Bank annual GDP**, and **ECB daily USD/EUR exchange rates** across Bronze, Silver, Gold, Warehouse, and Power BI layers.

---

## Core Engineering Principles

1. **Single Authoritative Month Selection:** One `months` parameter (explicit list or Bronze discovery via `nb_resolve_taxi_months`) drives ingestion, Silver transformations, Gold aggregations, and cross-layer reconciliation across all sources.
2. **Zero Silent Record Loss:** Source records are never silently discarded. In-scope anomalies are preserved and flagged; invalid-timeline or out-of-month records are quarantined with explicit reasons; every Bronze-to-Silver-to-Gold boundary reconciles row counts (`diff = 0`).
3. **Idempotent Month-Scoped Reprocessing:** All monthly Silver and Gold tables use Delta `replaceWhere` by month/date range so reprocessing one month (or nonconsecutive months) never duplicates rows or alters untouched history.
4. **Explicit Analytical Boundaries:**
   - Zero-distance taxi trips remain `0.0` (never `NULL`) and are included in `avg_distance_miles`, with `avg_positive_distance_miles` provided as a comparison.
   - Daily PM2.5 represents an **8-monitor AirNow network estimate** (requiring ≥75% valid hourly coverage per sensor and ≥4 eligible sensors; days below threshold remain `NULL`, never `0`).
   - Taxi revenue in EUR uses ECB's **last-business-day carry-forward** rate on weekends/holidays and retains the actual `rate_observation_date`.

---

## Architecture

```mermaid
flowchart LR
    subgraph Sources["External Data Sources"]
        TLC["NYC TLC Parquet + Zone CSV"]
        OAQ["OpenAQ v3 API (8 AirNow Monitors)"]
        WB["World Bank v2 API (NY.GDP.MKTP.CD)"]
        ECB["ECB Data API (EXR/D.USD.EUR.SP00.A)"]
    end

    subgraph Master["Orchestration: pl_nyc_analytics_master"]
        RES["nb_resolve_taxi_months"]
    end

    subgraph Bronze["lh_bronze (Raw + Normalized Delta)"]
        B1["Files/taxi/*.parquet + taxi_zone_lookup.csv"]
        B2["nb_ingest_openaq -> raw JSON + bronze_openaq_*"]
        B3["nb_ingest_world_bank -> bronze_world_bank_gdp"]
        B4["nb_ingest_ecb_fx -> bronze_ecb_fx"]
    end

    subgraph Silver["lh_silver (Classified + Quarantined + Audited)"]
        S1["nb_taxi_silver -> silver_taxi + quarantine + audit"]
        S2["nb_build_silver_air_quality -> silver_air_quality_* + audit"]
        S3["nb_build_silver_economics -> silver_world_bank_gdp + silver_ecb_fx + audit"]
    end

    subgraph Gold["lh_gold (Star Schema + Run Log)"]
        G1["nb_taxi_gold -> dim_date, dim_taxi_zone, fact_taxi_daily, fact_taxi_hourly_zone"]
        G2["nb_air_quality_gold -> dim_air_quality_sensor, fact_air_quality_daily_*"]
        G3["nb_economics_gold -> dim_currency, dim_country, fact_gdp_annual, fact_fx_daily"]
        G4["nb_taxi_air_quality_gold -> fact_taxi_air_quality_daily"]
        VAL["nb_validate_pipeline_run -> etl_run_log"]
    end

    subgraph Serve["Warehouse & Analytics"]
        WH["wh_nyc_analytics (18 Star-Schema Views)"]
        VIS["nb_analytics_insights (Charts & KPIs)"]
    end

    RES --> TLC & OAQ & WB & ECB
    TLC --> B1 --> S1 --> G1
    OAQ --> B2 --> S2 --> G2
    WB --> B3 --> S3 --> G3
    ECB --> B4 --> S3
    G1 & G2 --> G4
    G4 & G3 --> VAL
    G1 & G2 & G3 & G4 & VAL --> WH
    G1 & G2 & G3 & G4 --> VIS
```

---

## Verified Baseline Results (5-Month Period: `2025-11` to `2026-03`)

- **NYC Taxi Silver Reconciliation (`nb_taxi_silver`):**
  - `2025-11`: `4,181,444` Bronze = `4,179,985` Silver + `1,459` Quarantine (`diff = 0`)
  - `2025-12`: `4,305,006` Bronze = `4,304,995` Silver + `11` Quarantine (`diff = 0`)
  - `2026-01`: `3,724,889` Bronze = `3,724,881` Silver + `8` Quarantine (`diff = 0`)
  - `2026-02`: `3,399,866` Bronze = `3,399,848` Silver + `18` Quarantine (`diff = 0`)
  - `2026-03`: `3,952,451` Bronze = `3,952,430` Silver + `21` Quarantine (`diff = 0`)
  - `7,736` rows that legacy filters would have silently dropped are now explicitly preserved, flagged, or quarantined.
- **OpenAQ Silver Reconciliation (`nb_build_silver_air_quality`):**
  - `25,168` in-scope hourly records across 5 months = `25,066` accepted (`CONFIRMED_UNFLAGGED`) + `102` quarantined (`37` `FLAGGED`, `65` `MISSING_VALUE,FLAGGED`; `diff = 0`).
- **Combined Gold (`nb_taxi_air_quality_gold`):**
  - `151` daily records (`132` days with reliable PM2.5 network estimates; `19` days in `2025-11` below minimum coverage kept as `NULL`; `0` duplicate `date_key`s).
- **Master Pipeline (`pl_nyc_analytics_master`):** Verified end-to-end in both single-month explicit mode and full-period Bronze discovery mode.

---

## Repository Structure

| Component | Purpose | Link |
| --- | --- | --- |
| Master Pipeline | Orchestrates month resolution, parallel ingestion, Silver, Gold, and validation | [pipeline-content.json](pl_nyc_analytics_master.DataPipeline/pipeline-content.json) |
| Taxi Ingestion Pipeline | Downloads monthly Yellow Taxi Parquet files with retries | [pipeline-content.json](pl_ingest_taxi.DataPipeline/pipeline-content.json) |
| Month Resolver | Validates explicit `MONTHS_JSON` or discovers Bronze Parquet months | [nb_resolve_taxi_months](nb_resolve_taxi_months.Notebook/notebook-content.py) |
| OpenAQ Ingestion | Fetches OpenAQ v3 hourly measurements, metadata, and flags; saves raw JSON + Delta | [nb_ingest_openaq](nb_ingest_openaq.Notebook/notebook-content.py) |
| World Bank Ingestion | Fetches `NY.GDP.MKTP.CD` annual GDP JSON and writes Bronze Delta | [nb_ingest_world_bank](nb_ingest_world_bank.Notebook/notebook-content.py) |
| ECB FX Ingestion | Fetches daily `EXR/D.USD.EUR.SP00.A` CSV with lookback buffer and writes Bronze Delta | [nb_ingest_ecb_fx](nb_ingest_ecb_fx.Notebook/notebook-content.py) |
| Taxi Silver | Classifies quality flags, quarantines invalid/out-of-month trips, audits counts | [nb_taxi_silver](nb_taxi_silver.Notebook/notebook-content.py) |
| Air Quality Silver | Tri-state `flag_status`, multi-reason quarantine, 11-sensor audit, month-scoped writes | [nb_build_silver_air_quality](nb_build_silver_air_quality.Notebook/notebook-content.py) |
| Economics Silver | Cleans, classifies, and reconciles World Bank GDP and ECB FX | [nb_build_silver_economics](nb_build_silver_economics.Notebook/notebook-content.py) |
| Taxi Gold | Builds `dim_date`, `dim_taxi_zone`, `fact_taxi_daily`, and `fact_taxi_hourly_zone` | [nb_taxi_gold](nb_taxi_gold.Notebook/notebook-content.py) |
| Air Quality Gold | Builds `dim_air_quality_sensor`, DST-aware `fact_air_quality_daily_sensor`, daily & hourly citywide facts | [nb_air_quality_gold](nb_air_quality_gold.Notebook/notebook-content.py) |
| Economics Gold | Builds `dim_currency`, `dim_country`, `fact_gdp_annual`, and carry-forward `fact_fx_daily` | [nb_economics_gold](nb_economics_gold.Notebook/notebook-content.py) |
| Combined Gold | Month-scoped left join of `fact_taxi_daily` and `fact_air_quality_daily_citywide` | [nb_taxi_air_quality_gold](nb_taxi_air_quality_gold.Notebook/notebook-content.py) |
| Final Validation & Logging | Reconciles all layers and writes `lh_gold.dbo.etl_run_log` | [nb_validate_pipeline_run](nb_validate_pipeline_run.Notebook/notebook-content.py) |
| Analytical Visualizations | Generates Phase 4 charts & answers all 4 analytical questions | [nb_analytics_insights](nb_analytics_insights.Notebook/notebook-content.py) |
| Warehouse Star Schema | Fabric Warehouse SQL project & 18 star-schema/audit views | [wh_nyc_analytics.sqlproj](wh_nyc_analytics.Warehouse/wh_nyc_analytics.sqlproj) |
| Docs: Data Dictionary & Lineage | Table schemas, grains, and source-to-target mappings | [data_dictionary_and_lineage.md](docs/data_dictionary_and_lineage.md) |
| Docs: Governance & Quality Rules | Quality classifications, TLC/ECB/OpenAQ assumptions, and operations runbook | [governance_and_quality_rules.md](docs/governance_and_quality_rules.md) |

---

## How to Run in Microsoft Fabric

Trigger `pl_nyc_analytics_master` with one of two mutually exclusive modes:

- **Explicit months (supports new Taxi downloads):**
  ```json
  {
    "months": ["2025-11", "2025-12", "2026-01", "2026-02", "2026-03"],
    "discover_months": false,
    "ingest_taxi": true,
    "ingest_openaq": true,
    "ingest_economics": true
  }
  ```
- **Bronze discovery (reprocesses all valid Parquet months already in `lh_bronze/Files/taxi`):**
  ```json
  {
    "months": [],
    "discover_months": true,
    "ingest_taxi": false,
    "ingest_openaq": true,
    "ingest_economics": true
  }
  ```
