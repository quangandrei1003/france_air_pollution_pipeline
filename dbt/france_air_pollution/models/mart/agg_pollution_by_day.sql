{{ config(
    materialized='incremental', 
    on_schema_change='fail'
)}}

WITH pollution AS (
    SELECT *
    FROM {{ ref('fact_air_pollution') }}
    WHERE 
        pollution_quality IN ('Very_Poor', 'Poor', 'Moderate', 'Fair', 'Good')    
), 

dim_cities as (
    SELECT  
        *
    FROM 
    {{ ref('dim_cities') }}
)


SELECT 
    city,
    region,
    date,
    CASE 
        WHEN COUNTIF(pollution_quality = 'Very_Poor') > 0 THEN 'Very_Poor'
        WHEN COUNTIF(pollution_quality = 'Poor') > 0 THEN 'Poor'
        WHEN COUNTIF(pollution_quality = 'Moderate') > 0 THEN 'Moderate'
        WHEN COUNTIF(pollution_quality = 'Fair') > 0 THEN 'Fair'
        WHEN COUNTIF(pollution_quality = 'Good') > 0 THEN 'Good'
        ELSE 'Unknown'
    END AS pollution_quality,
    COUNT(DISTINCT date) AS count_days,
    ROUND(AVG(sulfur_dioxide_so2),2) as avg_SO2,
    ROUND(AVG(nitrogen_dioxide_no2),2) as avg_NO2,
    ROUND(AVG(pm10),2) as avg_PM10,
    ROUND(AVG(pm2_5),2) as avg_PM2_5,
    ROUND(AVG(ozone_o3),2) as avg_O3,
    ROUND(AVG(carbon_monoxide_co),2) as avg_CO,
    ROUND(AVG(nh3),2) as avg_NH3,
    ROUND(AVG(nitric_oxide_no),2) as avg_NO
FROM 
    pollution p 
LEFT JOIN 
    dim_cities c 
ON 
  p.city_id = c.city_id
WHERE 1=1
    {% if is_incremental() %}
    AND DATE(dt, 'Europe/Paris') > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
GROUP BY 
    1, 2, 3
ORDER BY 
    city,
    date