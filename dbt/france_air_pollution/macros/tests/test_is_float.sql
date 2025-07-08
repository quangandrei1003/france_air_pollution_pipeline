{% test test_is_float(model, column_name, expected_data_type) %}

WITH actual_column AS (
    SELECT
        column_name,
        data_type
    FROM `{{ target.project }}.{{ target.dataset }}.INFORMATION_SCHEMA.COLUMNS`
    WHERE
        table_name = LOWER('{{ model.name }}')
        AND column_name = LOWER('{{ column_name }}')
),

validation_errors AS (
    SELECT *
    FROM actual_column
    WHERE LOWER(data_type) != LOWER('{{ expected_data_type }}')
)

SELECT *
FROM validation_errors

{% endtest %}
