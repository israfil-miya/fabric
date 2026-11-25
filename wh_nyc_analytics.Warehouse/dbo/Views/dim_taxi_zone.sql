CREATE   VIEW dbo.dim_taxi_zone AS
SELECT
    location_id,
    borough,
    zone_name,
    service_zone,
    is_known_zone
FROM lh_gold.dbo.dim_taxi_zone;

GO