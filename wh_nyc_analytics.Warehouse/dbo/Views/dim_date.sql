-- =============================================================================
-- wh_nyc_analytics: Microsoft Fabric Warehouse Star Schema & Audit Views
-- =============================================================================
-- Prerequisites:
--   1. Create Warehouse `wh_nyc_analytics` in the `nyc_analytics` Fabric workspace.
--   2. Ensure `lh_gold` and `lh_silver` exist in the same workspace and have been
--      populated by `pl_nyc_analytics_master`.
--   3. Execute this script inside `wh_nyc_analytics`.
--
-- Design notes:
--   * Views query `lh_gold` / `lh_silver` via cross-database three-part names so
--     Warehouse data stays automatically current after each pipeline run without
--     duplicating Delta storage.
--   * Star schema uses shared conformed dimensions (`dim_date`, `dim_taxi_zone`,
--     `dim_air_quality_sensor`, `dim_currency`, `dim_country`) with NO direct
--     fact-to-fact relationships.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Conformed Dimensions
-- -----------------------------------------------------------------------------

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