-- -----------------------------------------------------------------------------
-- 3. Analytical Conversion View: Daily Taxi Revenue in USD and EUR
--    Applies ECB's last-business-day carry-forward rate and retains rate_observation_date.
-- -----------------------------------------------------------------------------

CREATE   VIEW dbo.vw_taxi_revenue_usd_eur_daily AS
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