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
    DATETIME(creation_time),
    IFNULL(forecast_hours, TIMESTAMP_DIFF(forecast_time, creation_time, HOUR)),
    DATETIME(forecast_time),
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
    `{{ source_project_id }}.{{ source_dataset }}.{{ source_table }}`;

DELETE FROM `{{ destination_project_id }}.{{ destination_dataset }}.{{ destination_table }}`
WHERE creation_time='{{ creation_time }}';

INSERT INTO `{{ destination_project_id }}.{{ destination_dataset }}.{{ destination_table }}`
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