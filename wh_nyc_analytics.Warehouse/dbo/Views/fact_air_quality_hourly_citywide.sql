CREATE   VIEW dbo.fact_air_quality_hourly_citywide AS
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