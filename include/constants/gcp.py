
from typing import Literal

# gcs
GCS_URI_SCHEME = "gs://"
GCS_BASE_PREFIX = "data/air_pollution"

# File formats
PARQUET_EXTENSION = "parquet"

# BigQuery
BQ_DATASET_RAW = "raw"
BQ_DATASET_DBT_DEV = "development"
BQ_DATASET_DBT_PROD = "production"

BQ_RAW_AIR_POLLUTION_TABLE_NAME = "airpollution"
BQ_RAW_CITIES_TABLE_NAME = "cities"
BQ_DATASET_LOCATION = "europe-west2"

# BigQuery job configuration
BQ_LOAD_JOB_CONFIG = {
    "load": {
        "sourceUris": None,
        "destinationTable": {
            "projectId": None,
            "datasetId": BQ_DATASET_RAW,
            "tableId": BQ_RAW_AIR_POLLUTION_TABLE_NAME,
        },
        "sourceFormat": PARQUET_EXTENSION.upper(),
        "autodetect": True,
        "writeDisposition": "WRITE_APPEND",
        "location": BQ_DATASET_LOCATION
    }
}

WRITE_IF_EXISTS_BQ_MODE = Literal["append", "replace", "fail"]

GCS_POLLUTION_PATH_MAP = {
    "history": lambda city: f"{GCS_BASE_PREFIX}/history/{city}.{PARQUET_EXTENSION}",
    "lastest": lambda city, month, date: f"{GCS_BASE_PREFIX}/{city}/{month}/{date}.{PARQUET_EXTENSION}"
}
