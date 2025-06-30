from include.config.env_config import load_gcs_bucket_name
from include.constants.gcp import GCS_BASE_PREFIX, GCS_POLLUTION_PATH_MAP, GCS_URI_SCHEME
from include.utils.date_utils import get_month_year_prev_date_str, get_prev_date_str


def get_gcs_prev_date_prefix() -> str:
    return f"{GCS_URI_SCHEME}{load_gcs_bucket_name()}/{GCS_BASE_PREFIX}"


def build_gcs_pollution_path(city_name, is_history: bool = False):
    if is_history:
        return GCS_POLLUTION_PATH_MAP.get('history')(city_name)

    return GCS_POLLUTION_PATH_MAP.get('lastest')(city_name, get_month_year_prev_date_str(), get_prev_date_str())
