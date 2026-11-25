CREATE   VIEW dbo.fact_air_quality_daily_sensor AS
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