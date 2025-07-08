from include.constants.file_paths import AIRFLOW_HOME

DBT_PROFILE_NAME = "france_air_pollution"
DBT_PROJECT_NAME = "france_air_pollution"
DBT_PROFILES_YML_NAME = "profiles.yml"

DBT_PROFILE_CONFIG = {
    "profile_name": DBT_PROFILE_NAME,
    "profiles_yml_filepath": f"{AIRFLOW_HOME}/dags/dbt/{DBT_PROJECT_NAME}/{DBT_PROFILES_YML_NAME}",
}

DBT_OPERATOR_ARGS = {
    "install_deps": True,
}
