{% test test_latest_datetime_by_city(model, group_by_column, datetime_column, timezone) %}

WITH max_date_time_cte AS (
    SELECT 
        {{ group_by_column }} AS city, 
        MAX({{ datetime_column }}) AS max_date_time
    FROM {{ model }}
    GROUP BY {{ group_by_column }}
)

SELECT 
    city,
    max_date_time
FROM max_date_time_cte
WHERE 
    {% if datetime_column == 'dt' %}
        DATE(max_date_time, "{{ timezone }}") < DATE_SUB(CURRENT_DATE("{{ timezone }}"), INTERVAL 1 DAY)
    {% else %}
        max_date_time < DATE_SUB(CURRENT_DATE("{{ timezone }}"), INTERVAL 1 DAY)
    {% endif %}
{% endtest %}
