CREATE   VIEW dbo.fact_gdp_annual AS
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