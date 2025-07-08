from airflow.decorators import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
from include.air_pollution.task_flow import (
    get_start_end_time_history_pollution,
    combine_cities_with_timerange,
    get_metropolitan_cities,
    get_air_pollution,
    transform_pollution_with_city,
    load_air_pollution_to_gcs,
    load_to_bigquery_task_group
)
from include.constants.file_paths import METROPOLITAN_CITIES_PATH
from include.constants.airflow import HISTORY_POLLUTION_DAG_PARAMS


@dag(
    dag_id='history_pollution',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    schedule_interval=None,
    tags=["history"],
    params=HISTORY_POLLUTION_DAG_PARAMS
)
def history_air_pollution():
    time_range = get_start_end_time_history_pollution(
        start_date="{{params.start_date}}")
    metropolitan_cities = get_metropolitan_cities(METROPOLITAN_CITIES_PATH)
    pollution_params = combine_cities_with_timerange(
        cities=metropolitan_cities, time_range=time_range)
    pollution_data = get_air_pollution.expand(city=pollution_params)
    formatted_data = transform_pollution_with_city.expand(
        payload=pollution_data)
    gcs_paths = load_air_pollution_to_gcs.expand(
        payload=formatted_data)
    load_to_bq = load_to_bigquery_task_group(gcs_paths=gcs_paths,
                                             metropolitan_cities=metropolitan_cities)
    trigger_dbt_transformation = TriggerDagRunOperator(
        task_id="dbt_transformation",
        trigger_dag_id="dbt_transform_bq",
        wait_for_completion=True,
        poke_interval=60,
        conf={"is_full_refresh": True},
    )

    gcs_paths >> load_to_bq >> trigger_dbt_transformation


dag = history_air_pollution()
