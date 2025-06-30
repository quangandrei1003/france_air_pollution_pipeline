import pendulum
from datetime import datetime
from include.constants.date_time import DATE_TIME_YYYY_MM_DD_FORMAT, YEAR_MONTH_YYYY_MM_FORMAT
from typing import Tuple


def get_today(tz: str = None) -> pendulum.DateTime:
    """
    Returns the current datetime using Pendulum, optionally in a specified timezone.

    If a timezone is provided, returns the current time in that timezone.
    Otherwise, returns the current time in the system's local timezone.

    Args:
        tz (str, optional): Timezone string (e.g. 'Europe/Paris', 'UTC').
            Defaults to None.

    Returns:
        pendulum.DateTime: The current datetime in the specified or local timezone.
    """
    if tz is None:
        return pendulum.now()

    tz = pendulum.timezone(tz)
    return pendulum.now(tz)


def get_current_date_str() -> str:
    """
    Returns current date in 'YYYY-MM-DD' format.
    """
    return datetime.now().strftime(DATE_TIME_YYYY_MM_DD_FORMAT)


def get_prev_date_str() -> str:
    """Returns the previous day's date in YYYY-MM-DD format."""
    return (get_today().subtract(days=1)).strftime(DATE_TIME_YYYY_MM_DD_FORMAT)


def get_month_year_str(date: datetime) -> str:
    """
    Returns current year and month in 'YYYY-MM' format.
    """
    return date.strftime(YEAR_MONTH_YYYY_MM_FORMAT)


def get_month_year_prev_date_str() -> str:
    """Returns the month and year of the previous date from a given date.

    Args:
        date (datetime): The reference date to get previous date from.

    Returns:
        str: Month and year of the previous date in format 'MM/YYYY'.
    """
    prev_date = get_today().subtract(days=1)
    return get_month_year_str(prev_date)


def get_prev_day_start_end_timestamps(tz: str) -> Tuple[int, int]:
    """
    Get the start and end timestamps for yesterday in a specified timezone.

    Args:
        tz (str): Timezone string (e.g. 'Europe/Paris', 'UTC')

    Returns:
        Tuple[int, int]: A tuple containing:
            - start_time (int): Unix timestamp for the start of yesterday (00:00:00)
            - end_time (int): Unix timestamp for the end of yesterday (23:59:59)

    Example:
        >>> get_yesterday_start_end_timestamps get_prev_day_start_end_timestamps 'Europe/Paris')
        (1677628800, 1677715199)
    """

    yesterday = get_today(tz=tz).subtract(days=1)

    start_time = int(yesterday.start_of('day').timestamp())
    end_time = int(yesterday.end_of('day').timestamp())

    return start_time, end_time
