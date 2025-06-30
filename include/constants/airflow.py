from include.constants.date_time import FIVE_MINUTE, PARIS_TZ
from include.utils.date_utils import get_today
from typing import Dict, Any

# airflow dag args, params config
PREV_DAY_DAG_ARGS: Dict[str, Any] = {
    'owner': 'airflow',
    'start_date': get_today(PARIS_TZ),
    'retries': 3,
    'retry_delay': FIVE_MINUTE
}

HISTORY_POLLUTION_DAG_PARAMS: Dict[str, Any] = {
    "start_date": "10-03-2021",
    "end_date": "28-06-2025"
    # end_date defaults to current time if not provided
}
