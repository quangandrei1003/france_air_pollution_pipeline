
import os
from dotenv import load_dotenv

load_dotenv()


def load_open_weather_api_key() -> str:
    """
    Load open weather api key from enviroment file
    """
    api_key = os.environ.get("API_KEY")
    return api_key


def load_gcp_project_id() -> str:
    """
    Load gcp project id from enviroment file
    """
    gcp_project_id = os.environ.get("GCP_PROJECT_ID")
    return gcp_project_id


def load_gcs_bucket_name() -> str:
    """
    Load gcs bucket name from enviroment file
    """
    gcs_bucket_name = os.environ.get("GCS_BUCKET_NAME")
    return gcs_bucket_name
