CREATE   VIEW dbo.fact_fx_daily AS
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