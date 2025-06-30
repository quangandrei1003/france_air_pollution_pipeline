from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import requests
import pandas as pd
import os
from typing import List, Union
import tempfile
from pathlib import Path
from include.config.env_config import load_gcp_project_id, load_open_weather_api_key
from include.constants.api import CURRENT_AIR_POLLUTION_ENDPOINT, HISTORY_AIR_POLLUTION_ENDPOINT
from include.constants.gcp import BQ_DATASET_LOCATION, WRITE_IF_EXISTS_BQ_MODE, BQ_LOAD_JOB_CONFIG


def fetch_current_air_pollution_data(
    lat: float, lon: float, start_date=None, end_date=None
) -> pd.DataFrame:
    """
    Get air pollution data for the current time for a specific latitude and longitude in a French city.

    Parameters
    ----------
    lat : float
        The latitude of the location.
    lon : float
        The longitude of the location.

    Returns
    -------
    df : pd.DataFrame
        A pandas DataFrame containing the retrieved air pollution data.
    """
    # API endpoint and API key
    api_endpoint = CURRENT_AIR_POLLUTION_ENDPOINT
    api_key = load_open_weather_api_key()

    # Make a request to the OpenWeatherMap API
    response = requests.get(
        api_endpoint,
        params={
            "lat": lat,
            "lon": lon,
            "appid": api_key,
        },
    )

    if response.status_code != 200:
        print("Error: API request failed with status code", response.status_code)
        exit()

    # Parse the API response
    data = response.json()
    # Convert the data to a dataframe
    df = pd.json_normalize(data, "list")

    return df


def fetch_history_air_pollution_data(
    start_time: int, end_time: int, city: dict
) -> pd.DataFrame:
    """
    Retrieve air pollution data from the OpenWeatherMap API for a specified time range and location.
    Parameters:
        start_time (int): The start time for the data range, SECONDS SINCE JAN 01 1970. (UTC).
        end_time (int): The end time for the data range, SECONDS SINCE JAN 01 1970. (UTC).
        lat (float): The latitude of the location for which to retrieve data.
        lon (float): The longitude of the location for which to retrieve data.
    Returns:
        pandas.DataFrame: A dataframe containing the API response data.
    """

    # API endpoint and API key
    api_endpoint = HISTORY_AIR_POLLUTION_ENDPOINT
    api_key = load_open_weather_api_key()

    # Send the API request
    response = requests.get(
        api_endpoint,
        params={
            "lat": city["latitude"],
            "lon": city["longitude"],
            "start": start_time,
            "end": end_time,
            "appid": api_key,
        },
    )

    # Check for errors
    if response.status_code != 200:
        print("Error: API request failed with status code", response.status_code)
        exit()

    # Parse the API response
    data = response.json()
    # Convert the data to a dataframe
    df = pd.json_normalize(data["list"])
    df['city_Index'] = city['city_index']

    return df


def df_to_parquet(df: pd.DataFrame) -> str:
    """Convert DataFrame to parquet file using a temporary file"""
    try:
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
            df.to_parquet(
                temp_file.name,
                engine="pyarrow",
                index=False,
                coerce_timestamps='us',
            )
            return temp_file.name
    except Exception as e:
        raise Exception(f"Failed to create parquet file: {str(e)}")


def write_gcs(local_path: str, gcs_path: str, bucket_name: str) -> str:
    """Upload parquet file to GCS and clean up temp file"""
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')

    try:
        gcs_hook.upload(
            bucket_name=bucket_name,
            object_name=gcs_path,
            filename=local_path
        )
        os.unlink(local_path)
        return f"gs://{bucket_name}/{gcs_path}"
    except Exception as e:
        if os.path.exists(local_path):
            os.unlink(local_path)  # Clean up temp file on error
        raise Exception(f"Failed to upload to GCS: {str(e)}")


def write_local(df: pd.DataFrame, path: str) -> None:
    Path(f"data/air_pollution").mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, compression="gzip")
    return path


def write_bq(
    df: pd.DataFrame,
    table: str,
    if_exists: WRITE_IF_EXISTS_BQ_MODE
) -> None:
    """
    Writes a Pandas DataFrame to BigQuery using a BigQuery hook for authentication.
        Args:
        df (pd.DataFrame): The DataFrame containing data to be written to BigQuery.

        table (str): The destination table name in BigQuery in the format 'dataset.table'.

        if_exists (WRITE_IF_EXISTS_BQ_MODE): The behavior when the destination table exists. 

            Valid values are 'fail', 'replace', or 'append'.
        Exception: If the function fails to write data to BigQuery, with details of the error.

    Example:
        >>> df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
        >>> write_bq(df, 'my_dataset.my_table', if_exists='replace')
    """
    bq_hook = BigQueryHook(gcp_conn_id="google_cloud_default")
    gcp_project_id = load_gcp_project_id()

    try:
        df.to_gbq(
            destination_table=table,
            project_id=gcp_project_id,
            credentials=bq_hook.get_credentials(),
            chunksize=500_000,
            if_exists=if_exists,
        )
    except Exception as e:
        raise Exception(f"Failed to write to BigQuery: {str(e)}")


def get_current_pollution(lat: float, lon: float) -> pd.DataFrame:
    """
    Retrieve air pollution data for the current time for a specific latitude and longitude.

    Parameters
    ----------
    lat : float
        The latitude of the location for which to retrieve data.
    lon : float
        The longitude of the location for which to retrieve data.

    Returns
    -------
    df : pd.DataFrame
        A pandas DataFrame containing the retrieved air pollution data.
    """
    # API endpoint and API key
    api_endpoint = "https://api.openweathermap.org/data/2.5/air_pollution"
    api_key = os.environ["API_KEY"]

    # Make a request to the OpenWeatherMap API
    response = requests.get(
        api_endpoint,
        params={
            "lat": lat,
            "lon": lon,
            "appid": api_key,
        },
    )

    # Check for errors
    if response.status_code != 200:
        print("Error: API request failed with status code", response.status_code)
        exit()

    # Parse the API response
    data = response.json()

    # Convert the data to a dataframe
    df = pd.json_normalize(data, "list", [["coord", "lon"], ["coord", "lat"]])

    return df


def gcs_to_bq(gcs_uri: Union[str, List[str]]) -> None:
    """
    Load data from GCS to BigQuery
    Args:
        gcs_uri: Either a wildcard path or list of GCS URIs
    """
    hook = BigQueryHook(location=BQ_DATASET_LOCATION)
    gcp_project_id = load_gcp_project_id()

    job_config = BQ_LOAD_JOB_CONFIG.copy()
    job_config["load"]["sourceUris"] = [
        gcs_uri] if isinstance(gcs_uri, str) else gcs_uri
    job_config["load"]["destinationTable"]["projectId"] = gcp_project_id

    try:
        hook.insert_job(configuration=job_config)
        print(f"Successfully loaded to BigQuery from gcs")
    except Exception as e:
        raise Exception(f"Failed to load from gcs to BigQuery: {str(e)}")
