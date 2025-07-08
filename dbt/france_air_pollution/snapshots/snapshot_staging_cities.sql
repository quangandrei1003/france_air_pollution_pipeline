{% snapshot snapshot_staging_cities %}

{{
    config(
        target_schema = (
        'production' if target.name == 'prod'
        else 'development'),
        unique_key='city_id',
        strategy = 'check',
        check_cols=['city', 'region'],
        invalidate_hard_deletes=True,
    )
}}

SELECT * FROM {{ ref('staging_cities') }}

{% endsnapshot %}
