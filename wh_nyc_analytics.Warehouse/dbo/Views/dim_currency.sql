CREATE   VIEW dbo.dim_currency AS
SELECT
    currency_code,
    currency_name,
    symbol,
    issuing_authority,
    is_reference_currency
FROM lh_gold.dbo.dim_currency;

GO