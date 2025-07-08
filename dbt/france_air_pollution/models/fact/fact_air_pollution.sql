{{ config(
    materialized='incremental', 
    on_schema_change='fail'
)}}

WITH staging_pollution AS (
    SELECT 
        *
    FROM 
        {{ ref('staging_air_pollution') }}
    {% if is_incremental() %}
    WHERE dt > (SELECT MAX(dt) FROM {{ this }})
    {% endif %}
)

SELECT 
    city_id,
    dt,
    date,
    pollution_quality,
    sulfur_dioxide_so2,
    nitrogen_dioxide_no2,
    pm10,
    pm2_5,
    ozone_o3,
    carbon_monoxide_co,
    nh3,
    nitric_oxide_no
FROM 
    staging_pollution AS p
ORDER BY 
    dt

