CREATE   VIEW dbo.fact_air_quality_daily_citywide AS
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