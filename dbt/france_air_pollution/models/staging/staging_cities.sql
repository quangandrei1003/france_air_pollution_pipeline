WITH raw_cities AS (
    SELECT *,
    row_number() over(partition by city_index) AS rn
    FROM {{ source('raw','cities') }}
    ORDER BY city_index
)

SELECT 
    city_index AS city_id,
    city, 
    latitude, 
    longitude, 
    region
FROM 
    raw_cities