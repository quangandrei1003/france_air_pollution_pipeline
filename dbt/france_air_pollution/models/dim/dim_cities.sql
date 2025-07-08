WITH staging_cities AS (
    SELECT * 
    FROM {{ ref('snapshot_staging_cities') }}
)

SELECT 
    city_id, 
    city, 
    latitude, 
    longitude, 
    region
FROM 
    staging_cities 
WHERE 
    dbt_valid_to IS NULL