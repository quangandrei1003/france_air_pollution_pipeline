from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
import os
from datetime import datetime
from pathlib import Path
from include.constants.dbt import DBT_PROFILE_CONFIG, DBT_PROJECT_NAME, DBT_OPERATOR_ARGS
from include.constants.file_paths import AIRFLOW_HOME, DBT_ROOT_FOLDER, DBT_VIRTUAL_EXE_PATH
from include.constants.date_time import ONE_MINUTE

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / DBT_ROOT_FOLDER
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))
PROJECT_PATH = DBT_ROOT_PATH / DBT_PROJECT_NAME
DBT_EXE_PATH = f"{AIRFLOW_HOME}/{DBT_VIRTUAL_EXE_PATH}"


@dag(
    dag_id="dbt_transform_bq",
    start_date=datetime(2024, 10, 18),
    catchup=False,
    max_consecutive_failed_dag_runs=10,
    default_args={
        "retries": 3,
        "retry_delay": ONE_MINUTE,
    },
)
def dbt_transform_bq():

    @task()
    def get_full_refresh_flag():
        context = get_current_context()
        return context["dag_run"].conf.get("is_full_refresh", False)

    full_refresh_flag = get_full_refresh_flag()

    transform_bq_dev = DbtTaskGroup(
        group_id="dbt_dev",
        project_config=ProjectConfig(PROJECT_PATH),
        profile_config=ProfileConfig(
            **DBT_PROFILE_CONFIG,
            target_name="dev"
        ),
        operator_args={
            **DBT_OPERATOR_ARGS,
            "full_refresh": get_full_refresh_flag()
        },
        execution_config=ExecutionConfig(
            dbt_executable_path=DBT_EXE_PATH,
        ),
        render_config=RenderConfig(),
    )

    transform_bq_prod = DbtTaskGroup(
        group_id="dbt_prod",
        project_config=ProjectConfig(PROJECT_PATH),
        profile_config=ProfileConfig(
            **DBT_PROFILE_CONFIG,
            target_name="prod"
        ),
        operator_args={
            **DBT_OPERATOR_ARGS,
            "full_refresh": get_full_refresh_flag()
        },
        execution_config=ExecutionConfig(
            dbt_executable_path=DBT_EXE_PATH
        ),
        render_config=RenderConfig(),
    )

    full_refresh_flag >> transform_bq_dev >> transform_bq_prod


dag = dbt_transform_bq()
