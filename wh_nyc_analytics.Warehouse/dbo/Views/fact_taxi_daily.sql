-- -----------------------------------------------------------------------------
-- 2. Fact Tables (Star Schema)
-- -----------------------------------------------------------------------------

CREATE   VIEW dbo.fact_taxi_daily AS
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