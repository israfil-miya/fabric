CREATE   VIEW dbo.dim_air_quality_sensor AS
SELECT
    sensor_id,
    location_id,
    location_name,
    locality,
    country,
    timezone,
    provider_name,
    owner_name,
    is_monitor,
    is_mobile,
    instrument_name,
    latitude,
    longitude,
    first_observation_utc,
    last_observation_utc
FROM lh_gold.dbo.dim_air_quality_sensor;

GO