CREATE   VIEW dbo.dim_country AS
SELECT
    country_iso3,
    country_code_2,
    country_name,
    primary_currency_code,
    entity_type
FROM lh_gold.dbo.dim_country;

GO