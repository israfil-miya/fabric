-- Cross-database views over lh_gold and lh_silver keep Warehouse queries
-- current after each pipeline run without duplicating Delta storage.
-- Fact views join only through conformed dimensions, never fact-to-fact.

-- Dimensions

CREATE   VIEW dbo.dim_date AS
SELECT
    date_key,
    full_date,
    [year],
    [quarter],
    [month],
    month_name,
    [day],
    day_of_week,
    day_name,
    is_weekend,
    year_month
FROM lh_gold.dbo.dim_date;

GO