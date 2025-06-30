from airflow.decorators import dag
from include.air_pollution.air_pollution_task_flow import (
    get_air_pollution,
    get_metropolitan_cities,
    transform_pollution_with_city,
    load_air_pollution_to_gcs,
    load_gcs_to_bigquery
)
from include.constants.file_paths import METROPOLITAN_CITIES_PATH
from include.constants.airflow import PREV_DAY_DAG_ARGS
from include.constants.date_time import CRON_DAILY_0015


@dag(
    dag_id='prev_day_air_pollution',
    default_args=PREV_DAY_DAG_ARGS,
    catchup=False,
    schedule_interval=CRON_DAILY_0015,  # Run at 00:15 AM at PARIS_TZ
    tags=["prev_day"]
)
def prev_day_air_pollution():
    metropolitan_cities = get_metropolitan_cities(METROPOLITAN_CITIES_PATH)
    pollution_data = get_air_pollution.expand(city=metropolitan_cities)
    formatted_data = transform_pollution_with_city.expand(
        payload=pollution_data)
    gcs_paths = load_air_pollution_to_gcs.expand(
        payload=formatted_data)
    load_gcs_to_bigquery(gcs_paths)


dag = prev_day_air_pollution()
