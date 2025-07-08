from include.config.env_config import load_gcs_bucket_name
from include.constants.gcp import GCS_BASE_PREFIX, GCS_POLLUTION_PATH_MAP, GCS_URI_SCHEME
from include.utils.date_utils import get_month_year_prev_date_str, get_prev_date_str


def get_gcs_prev_date_prefix() -> str:
    """
    Gets the Google Cloud Storage (GCS) prefix path for the previous date.
    This function constructs a URI prefix for GCS by combining:
    - The GCS URI scheme ('gs://')
    - The configured bucket name
    - The base prefix path
    Returns:
        str: The complete GCS prefix path in the format 'gs://bucket-name/base-prefix'
    """
    return f"{GCS_URI_SCHEME}{load_gcs_bucket_name()}/{GCS_BASE_PREFIX}"


def build_gcs_pollution_path(city_name, is_history: bool = False):
    """
    Builds the Google Cloud Storage (GCS) path for pollution data based on the city and data type.
    This function constructs the appropriate GCS path for either historical or latest pollution data
    for a specified city using predefined path mapping patterns.
    Args:
        city_name (str): Name of the city for which to build the GCS path
        is_history (bool, optional): Flag to determine if historical path should be built. Defaults to False.
    Returns:
        str: The constructed GCS path string for the specified city and data type.
            For historical data: Uses the 'history' path pattern
            For latest data: Uses the 'latest' path pattern with current month/year and previous date
    """

    if is_history:
        return GCS_POLLUTION_PATH_MAP.get('history')(city_name)

    return GCS_POLLUTION_PATH_MAP.get('lastest')(city_name, get_month_year_prev_date_str(), get_prev_date_str())
