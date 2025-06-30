from airflow.decorators import task, task_group
import pandas as pd
from datetime import datetime
from typing import List
from include.air_pollution.tasks import df_to_parquet, fetch_history_air_pollution_data, gcs_to_bq, write_gcs, write_bq
from include.config.env_config import load_gcs_bucket_name
from include.constants.date_time import ONE_MINUTE, PARIS_TZ
from include.constants.gcp import BQ_DATASET_RAW, BQ_RAW_CITIES_TABLE_NAME
from include.utils.date_utils import get_prev_day_start_end_timestamps
from include.utils.df_utils import clean_air_pollution_df, json_to_df, lowercase_df_columns
from include.utils.gcp_path_utils import build_gcs_pollution_path, get_gcs_prev_date_prefix
from include.constants.air_pollution import AIR_POLLUTION_COLUMN_MAP
from include.utils.str_utils import slugify


@task(retries=3, retry_delay=ONE_MINUTE)
def get_start_end_time_history_pollution(start_date: datetime, end_date: datetime = None):
    """Generates timestamps for air pollution data retrieval period.
    This function creates a dictionary containing start and end timestamps for querying
    air pollution historical data. If no end date is provided, it uses the current time
    as the end point.
    Args:
        start_date (datetime): The starting date in format 'DD-MM-YYYY'
        end_date (datetime, optional): The ending date in format 'DD-MM-YYYY'. 
            Defaults to None, in which case current time is used.

    Returns:
        dict: A dictionary containing:
            - 'start_time' (int): Unix timestamp for the start date
            - 'end_time' (int): Unix timestamp for the end date or current time

    Example:
        >>> get_start_end_time_history_pollution('01-01-2023')
        {'start_time': 1672531200, 'end_time': 1693526400}
    """

    time_range = {
        'start_time': int(datetime.strptime(start_date, "%d-%m-%Y").timestamp()),
        'end_time': int(datetime.strptime(start_date, "%d-%m-%Y").timestamp())
        if end_date else int(datetime.now().timestamp())
    }
    return time_range


@task(retries=3, retry_delay=ONE_MINUTE)
def combine_cities_with_timerange(cities: List[dict], time_range: dict) -> List[dict]:
    """
    Combine city data with time range parameters for pollution data fetch.

    Args:
        cities: List of city dictionaries
        time_range: Dictionary with start_time and end_time

    Returns:
        List[dict]: List of cities enriched with time parameters
    """
    return [{
        **city,
        "start_time": time_range['start_time'],
        "end_time": time_range['end_time']
    } for city in cities]


@task(retries=3, retry_delay=ONE_MINUTE)
def get_metropolitan_cities(path: str) -> List[dict]:
    """Load French metropolitan cities data from a JSON file.

    Args:
        path (str): Path to JSON file containing city data.

    Returns:
        List[dict]: List of cities with their coordinates and metadata.
            Each dict contains:
            - City (str): City name
            - City_index (str): City identifier
            - Latitude (float): City latitude
            - Longitude (float): City longitude
    """
    cities = pd.read_json(path)
    formatted_cities = lowercase_df_columns(df=cities)
    return formatted_cities.to_dict(orient="records")


@task(retries=3, retry_delay=ONE_MINUTE)
def get_air_pollution(city: dict) -> pd.DataFrame:
    """
    Retrieves historical air pollution data for a specified city within a defined date range.

    Parameters
    ----------
    city : dict
        Dictionary containing city information with:
            - city (str): city name
            - city_index (str): city identifier
            - latitude (float): city latitude
            - longitude (float): city longitude
            - start_time (int, optional): start timestamp for historical data. Defaults to the start timestamp of the previous day if not provided.
            - end_time (int, optional): end timestamp for historical data. Defaults to the end timestamp of the previous day if not provided.

    Returns
    -------
    dict
        A dictionary containing:
            - 'df': JSON string of the pollution data DataFrame in split orientation
            - 'city': Name of the city (str)
    """
    start_time, end_time = city.get('start_time'), city.get('end_time')

    if start_time is None and end_time is None:
        start_time, end_time = get_prev_day_start_end_timestamps(tz=PARIS_TZ)
    df_pollution = fetch_history_air_pollution_data(start_time=start_time,
                                                    end_time=end_time,
                                                    city=city)
    return {
        "df": df_pollution,
        "city": city['city']
    }


@task(retries=3, retry_delay=ONE_MINUTE)
def transform_pollution_with_city(payload: dict) -> dict:
    """Transform pollution DataFrame and format city name for storage"""
    transformed_df = clean_air_pollution_df(
        df=payload['df'],
        remove_columns=['main.aqi'],
        to_date_time_columns=['dt'],
        to_float_columns=list(AIR_POLLUTION_COLUMN_MAP.values())
    )

    return {
        "df": transformed_df,
        "city": slugify(payload["city"])
    }


@task(retries=3, retry_delay=ONE_MINUTE)
def load_air_pollution_to_gcs(payload: dict) -> None:
    """
    Loads air pollution data into Google Cloud Storage (GCS).

    This function processes a DataFrame containing air pollution data, cleans it,
    and uploads it to GCS as a Parquet file. The file is stored in a hierarchical
    path structure based on city name, month-year, and current date.

    Args:
        payload (dict): A dictionary containing:
        - 'df': JSON string representation of DataFrame with air pollution data
        - 'city': Name of the city for which data is being stored

    Returns:
        str: GCS path where the parquet file is stored
    """
    df, city_name = payload['df'], payload['city']
    gcs_path = build_gcs_pollution_path(city_name=city_name)
    air_pollution_parquet = df_to_parquet(df=df)
    upload_gcs_path = write_gcs(local_path=air_pollution_parquet,
                                gcs_path=gcs_path,
                                bucket_name=load_gcs_bucket_name())
    return upload_gcs_path


@task(retries=3, retry_delay=ONE_MINUTE)
def load_gcs_to_bigquery(paths: List[str]) -> None:
    """
    Collect all GCS paths from load_air_pollution_to_gcs task and load data into BigQuery.

    Args:
        paths (List[str]): Either a single GCS path wildcard (e.g. 'gs://bucket/folder/*') 
            or list of GCS paths containing parquet files to load into BigQuery.
            Paths must start with 'gs://'
        - Filters for valid GCS paths that start with configured GCS prefix
        - Uses gcs_to_bq utility function to load the parquet files into BigQuery

    Returns:
        None
    Examples:
        >> >  # Single wildcard path
        >> > load_gcs_to_bigquery('gs://my-bucket/folder/*.parquet')
        >> >  # List of paths
        >> > paths = ['gs://bucket/file1.parquet', 'gs://bucket/file2.parquet']
        >> > load_gcs_to_bigquery(paths)
    """

    gcs_prefix = get_gcs_prev_date_prefix()
    gcs_paths = [
        path for path in paths if path and path.startswith(gcs_prefix)]
    print(f"Number of gcs paths: {len(gcs_paths)}")
    return gcs_to_bq(gcs_paths)


@task(retries=3, retry_delay=ONE_MINUTE)
def write_cities_to_bigquery(cities: List[dict]) -> None:
    """Write cities data to BigQuery table.

    This function takes a list of city dictionaries, converts them to a pandas DataFrame,
    and writes the data to a specified BigQuery table, replacing any existing data.
    Args:
        cities (List[dict]): A list of dictionaries containing city data.
            Each dictionary represents a city with its attributes.
    Returns:
        None
    Raises:
        BigQueryError: If there is an error writing to BigQuery
    """

    cities_df = json_to_df(json=cities)
    write_bq(df=cities_df,
             table=f"{BQ_DATASET_RAW}.{BQ_RAW_CITIES_TABLE_NAME}",
             if_exists="replace")


@task_group(group_id="bigquery_task_group")
def load_to_bigquery_task_group(gcs_paths: List[str], metropolitan_cities: List[dict]):
    """Task group for loading data into BigQuery.
    This task group orchestrates the loading of city data and air pollution measurements
    into BigQuery tables. It first loads the metropolitan cities data and then loads
    the air pollution measurements from GCS paths.

    Args:
        gcs_paths (List[str]): List of Google Cloud Storage paths containing air pollution data files
        metropolitan_cities (List[dict]): List of dictionaries containing metropolitan city data

    Returns:
        None: This task group doesn't return any values, it performs BigQuery loading operations

    Example:
        gcs_paths = ['gs://bucket/file1.csv', 'gs://bucket/file2.csv']
        cities = [{'name': 'Paris', 'population': 2000000}, ...]
        load_to_bigquery_task_group(gcs_paths, cities)
    """
    write_cities_to_bigquery(cities=metropolitan_cities)
    load_gcs_to_bigquery(paths=gcs_paths)
