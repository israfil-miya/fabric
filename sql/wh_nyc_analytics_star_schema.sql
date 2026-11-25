-- =============================================================================
-- wh_nyc_analytics: Microsoft Fabric Warehouse Star Schema & Audit Views
-- =============================================================================
-- Prerequisites:
--   1. Create Warehouse `wh_nyc_analytics` in the `nyc_analytics` Fabric workspace.
--   2. Ensure `lh_gold` and `lh_silver` exist in the same workspace and have been
--      populated by `pl_nyc_analytics_master`.
--   3. Execute this script inside `wh_nyc_analytics`.
--
-- Design notes:
--   * Views query `lh_gold` / `lh_silver` via cross-database three-part names so
--     Warehouse data stays automatically current after each pipeline run without
--     duplicating Delta storage.
--   * Star schema uses shared conformed dimensions (`dim_date`, `dim_taxi_zone`,
--     `dim_air_quality_sensor`, `dim_currency`, `dim_country`) with NO direct
--     fact-to-fact relationships.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Conformed Dimensions
-- -----------------------------------------------------------------------------

CREATE OR ALTER VIEW dbo.dim_date AS
SELECT
    date_key,
    full_date,
    [year],
    [quarter],
    [month],
    month_name,
    [day],
    day_of_week,
    day_name,
    is_weekend,
    year_month
FROM lh_gold.dbo.dim_date;
GO

CREATE OR ALTER VIEW dbo.dim_taxi_zone AS
SELECT
    location_id,
    borough,
    zone_name,
    service_zone,
    is_known_zone
FROM lh_gold.dbo.dim_taxi_zone;
GO

CREATE OR ALTER VIEW dbo.dim_air_quality_sensor AS
SELECT
    sensor_id,
    location_id,
    location_name,
    locality,
    country,
    timezone,
    provider_name,
    owner_name,
    is_monitor,
    is_mobile,
    instrument_name,
    latitude,
    longitude,
    first_observation_utc,
    last_observation_utc
FROM lh_gold.dbo.dim_air_quality_sensor;
GO

CREATE OR ALTER VIEW dbo.dim_currency AS
SELECT
    currency_code,
    currency_name,
    symbol,
    issuing_authority,
    is_reference_currency
FROM lh_gold.dbo.dim_currency;
GO

CREATE OR ALTER VIEW dbo.dim_country AS
SELECT
    country_iso3,
    country_code_2,
    country_name,
    primary_currency_code,
    entity_type
FROM lh_gold.dbo.dim_country;
GO

-- -----------------------------------------------------------------------------
-- 2. Fact Tables (Star Schema)
-- -----------------------------------------------------------------------------

CREATE OR ALTER VIEW dbo.fact_taxi_daily AS
SELECT
    date_key,
    trip_count,
    revenue_trip_count,
    total_fare_usd,
    total_amount_usd,
    nonnegative_fare_trip_count,
    gross_nonnegative_fare_usd,
    nonnegative_total_trip_count,
    gross_nonnegative_total_amount_usd,
    net_fare_usd,
    net_total_amount_usd,
    avg_distance_miles,
    avg_positive_distance_miles,
    avg_duration_min,
    zero_distance_trips,
    zero_distance_pct,
    missing_distance_trips,
    negative_distance_trips,
    unusual_distance_trips,
    zero_duration_trips,
    unusual_duration_trips,
    missing_dropoff_trips,
    negative_fare_trips,
    negative_total_trips,
    negative_amount_trips,
    missing_fare_trips,
    missing_total_trips,
    missing_amount_trips,
    missing_passenger_trips,
    missing_pu_location_trips,
    duplicate_record_trips,
    flex_fare_trip_count,
    credit_card_trip_count,
    cash_trip_count,
    no_charge_trip_count,
    dispute_trip_count,
    unknown_payment_trip_count,
    voided_trip_count,
    other_payment_trip_count,
    net_fare_flex_usd,
    net_total_amount_flex_usd,
    has_observed_trips
FROM lh_gold.dbo.fact_taxi_daily;
GO

CREATE OR ALTER VIEW dbo.fact_taxi_hourly_zone AS
SELECT
    date_key,
    pickup_hour,
    PULocationID,
    trip_count,
    revenue_trip_count,
    total_fare_usd,
    total_amount_usd,
    net_fare_usd,
    net_total_amount_usd,
    avg_distance_miles,
    avg_positive_distance_miles,
    avg_duration_min,
    zero_distance_trip_count,
    missing_distance_count,
    negative_distance_count,
    unusual_distance_count,
    negative_amount_trip_count,
    missing_amount_trip_count,
    quality_flagged_trip_count
FROM lh_gold.dbo.fact_taxi_hourly_zone;
GO

CREATE OR ALTER VIEW dbo.fact_air_quality_daily_sensor AS
SELECT
    CAST(CONVERT(CHAR(8), local_date, 112) AS INT) AS date_key,
    sensor_id,
    local_date,
    expected_hour_count,
    valid_hour_count,
    unflagged_hour_count,
    avg_pm25_all_valid,
    min_pm25_all_valid,
    max_pm25_all_valid,
    avg_pm25_unflagged,
    min_pm25_unflagged,
    max_pm25_unflagged,
    avg_source_hour_coverage_pct,
    quarantined_hour_count,
    flagged_hour_count,
    flagged_value_hour_count,
    unknown_flag_hour_count,
    missing_value_hour_count,
    other_quarantined_hour_count,
    source_observed_hour_count,
    hour_coverage_pct,
    unflagged_hour_coverage_pct,
    source_observed_hour_coverage_pct
FROM lh_gold.dbo.fact_air_quality_daily_sensor;
GO

CREATE OR ALTER VIEW dbo.fact_air_quality_daily_citywide AS
SELECT
    CAST(CONVERT(CHAR(8), local_date, 112) AS INT) AS date_key,
    local_date,
    available_sensor_count,
    eligible_sensor_count,
    min_sensor_avg_pm25,
    max_sensor_avg_pm25,
    avg_eligible_sensor_coverage_pct,
    network_mean_pm25,
    network_median_pm25,
    minimum_sensor_coverage_pct,
    minimum_eligible_sensor_count
FROM lh_gold.dbo.fact_air_quality_daily_citywide;
GO

CREATE OR ALTER VIEW dbo.fact_air_quality_hourly_citywide AS
SELECT
    date_key,
    local_date,
    local_hour,
    expected_utc_hours,
    available_sensor_count,
    hourly_network_mean_pm25,
    hourly_network_median_pm25,
    min_sensor_pm25,
    max_sensor_pm25,
    minimum_eligible_sensor_count
FROM lh_gold.dbo.fact_air_quality_hourly_citywide;
GO

CREATE OR ALTER VIEW dbo.fact_taxi_air_quality_daily AS
SELECT
    *
FROM lh_gold.dbo.fact_taxi_air_quality_daily;
GO

CREATE OR ALTER VIEW dbo.fact_fx_daily AS
SELECT
    date_key,
    full_date,
    currency_pair,
    base_currency,
    quote_currency,
    quote_direction,
    usd_per_eur_rate,
    eur_per_usd_rate,
    rate_observation_date,
    obs_status,
    unit,
    is_carry_forward_rate,
    days_since_rate_observation,
    rate_policy
FROM lh_gold.dbo.fact_fx_daily;
GO

CREATE OR ALTER VIEW dbo.fact_gdp_annual AS
SELECT
    country_iso3,
    country_name,
    [year],
    indicator_id,
    indicator_name,
    gdp_usd,
    last_updated_date,
    ingested_at_utc
FROM lh_gold.dbo.fact_gdp_annual;
GO

-- -----------------------------------------------------------------------------
-- 3. Analytical Conversion View: Daily Taxi Revenue in USD and EUR
--    Applies ECB's last-business-day carry-forward rate and retains rate_observation_date.
-- -----------------------------------------------------------------------------

CREATE OR ALTER VIEW dbo.vw_taxi_revenue_usd_eur_daily AS
SELECT
    t.date_key,
    fx.full_date,
    t.trip_count,
    t.revenue_trip_count,
    t.total_fare_usd,
    t.total_amount_usd,
    t.gross_nonnegative_fare_usd,
    t.gross_nonnegative_total_amount_usd,
    t.net_fare_usd,
    t.net_total_amount_usd,
    fx.currency_pair,
    fx.usd_per_eur_rate,
    fx.eur_per_usd_rate,
    fx.rate_observation_date,
    fx.is_carry_forward_rate,
    fx.days_since_rate_observation,
    fx.rate_policy,
    ROUND(t.total_fare_usd * fx.eur_per_usd_rate, 2) AS total_fare_eur,
    ROUND(t.total_amount_usd * fx.eur_per_usd_rate, 2) AS total_amount_eur,
    ROUND(t.gross_nonnegative_fare_usd * fx.eur_per_usd_rate, 2) AS gross_nonnegative_fare_eur,
    ROUND(t.gross_nonnegative_total_amount_usd * fx.eur_per_usd_rate, 2) AS gross_nonnegative_total_amount_eur,
    ROUND(t.net_fare_usd * fx.eur_per_usd_rate, 2) AS net_fare_eur,
    ROUND(t.net_total_amount_usd * fx.eur_per_usd_rate, 2) AS net_total_amount_eur
FROM lh_gold.dbo.fact_taxi_daily AS t
INNER JOIN lh_gold.dbo.fact_fx_daily AS fx
    ON t.date_key = fx.date_key
   AND fx.currency_pair = 'EUR/USD';
GO

-- -----------------------------------------------------------------------------
-- 4. Data Quality & Pipeline Monitoring Views
-- -----------------------------------------------------------------------------

CREATE OR ALTER VIEW dbo.vw_audit_taxi_quality AS
SELECT * FROM lh_silver.dbo.silver_taxi_quality_audit;
GO

CREATE OR ALTER VIEW dbo.vw_audit_air_quality AS
SELECT * FROM lh_silver.dbo.silver_air_quality_quality_audit;
GO

CREATE OR ALTER VIEW dbo.vw_audit_economics AS
SELECT * FROM lh_silver.dbo.silver_economics_quality_audit;
GO

CREATE OR ALTER VIEW dbo.vw_etl_run_log AS
SELECT * FROM lh_gold.dbo.etl_run_log;
GO
