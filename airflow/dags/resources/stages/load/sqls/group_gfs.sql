CREATE TEMP TABLE temp_gfs(
    geography STRING,
    geography_polygon STRING,
    creation_time DATETIME,
    forecast_hours INTEGER,
    forecast_time DATETIME,
    temperature_2m_above_ground FLOAT64,
    specific_humidity_2m_above_ground FLOAT64,
    relative_humidity_2m_above_ground FLOAT64,
    u_component_of_wind_10m_above_ground FLOAT64,
    v_component_of_wind_10m_above_ground FLOAT64,
    total_precipitation_surface FLOAT64,
    precipitable_water_entire_atmosphere FLOAT64,
    total_cloud_cover_entire_atmosphere FLOAT64,
    downward_shortwave_radiation_flux FLOAT64)
AS
SELECT
    ST_ASTEXT(geography),
    ST_ASTEXT(geography_polygon),
    creation_time,
    forecast_hours,
    forecast_time,
    temperature_2m_above_ground,
    specific_humidity_2m_above_ground,
    relative_humidity_2m_above_ground,
    u_component_of_wind_10m_above_ground,
    v_component_of_wind_10m_above_ground,
    total_precipitation_surface,
    precipitable_water_entire_atmosphere,
    total_cloud_cover_entire_atmosphere,
    downward_shortwave_radiation_flux
FROM
    `{{ project_id }}.{{ dataset_temp }}.{{ table_temp }}`;

INSERT INTO `{{ project_id }}.{{ dataset }}.{{ table }}`
(
    creation_time,
    geography,
    geography_polygon,
    forecast
)
SELECT
    creation_time,
    ST_GEOGFROMTEXT(geography),
    ST_GEOGFROMTEXT(geography_polygon),
    ARRAY_AGG( STRUCT(forecast_hours AS hours,
      forecast_time AS time,
      temperature_2m_above_ground,
      specific_humidity_2m_above_ground,
      relative_humidity_2m_above_ground,
      u_component_of_wind_10m_above_ground,
      v_component_of_wind_10m_above_ground,
      total_precipitation_surface,
      precipitable_water_entire_atmosphere,
      total_cloud_cover_entire_atmosphere,
      downward_shortwave_radiation_flux
      ) ORDER BY forecast_hours ASC) AS forecast
FROM
    `temp_gfs`
GROUP BY
      creation_time,
      geography,
      geography_polygon;

DROP TABLE temp_gfs;