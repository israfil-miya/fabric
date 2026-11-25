CREATE   VIEW dbo.fact_taxi_hourly_zone AS
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