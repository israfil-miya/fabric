-- -----------------------------------------------------------------------------
-- 4. Data Quality & Pipeline Monitoring Views
-- -----------------------------------------------------------------------------

CREATE   VIEW dbo.vw_audit_taxi_quality AS
SELECT * FROM lh_silver.dbo.silver_taxi_quality_audit;

GO