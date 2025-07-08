from datetime import UTC, datetime
from typing import List
import pandas as pd
from include.constants.air_pollution import AIR_POLLUTION_COLUMN_MAP


def json_to_df(json: List[dict]) -> pd.DataFrame:
    """
    Convert a list of dictionaries to a pandas DataFrame.

    Args:
        json (List[dict]): A list of dictionaries where each dictionary represents a row of data

    Returns:
        pd.DataFrame: A pandas DataFrame containing the data from the input list of dictionaries
    """
    df = pd.DataFrame(json)
    return df


def clean_air_pollution_df(df: pd.DataFrame,
                           remove_columns: List[str] = None,
                           to_date_time_columns: List[str] = None,
                           to_float_columns: List[str] = None) -> pd.DataFrame:
    """
    Clean a DataFrame containing air pollution data by performing various transformations.
    This function applies several cleaning operations on a DataFrame:
    1. Renames columns using a standard format
    2. Removes specified columns
    3. Converts specified columns to datetime format
    4. Converts specified columns to float format
    Args:
        df (pd.DataFrame): Input DataFrame containing air pollution data
        remove_columns (List[str], optional): List of column names to remove. Defaults to None.
        to_date_time_columns (List[str], optional): List of column names to convert to datetime. Defaults to None.
        to_float_columns (List[str], optional): List of column names to convert to float. Defaults to None.
    Returns:
        pd.DataFrame: Cleaned DataFrame with applied transformations
    Example:
        >>> df = pd.DataFrame(...)
        >>> clean_df = clean_air_pollution_df(
        ...     df,
        ...     remove_columns=['unnecessary_col'],
        ...     to_date_time_columns=['date'],
        ...     to_float_columns=['pollution_level']
        ... )
    """
    cleaned_df = df.copy()
    cleaned_df = rename_columns(cleaned_df)

    if remove_columns:
        cleaned_df = remove_df_columns(cleaned_df, remove_columns)

    if to_date_time_columns:
        cleaned_df = convert_date_time_columns(
            cleaned_df, to_date_time_columns)

    if to_float_columns:
        cleaned_df = convert_columns_to_float(cleaned_df, to_float_columns)

    return cleaned_df


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename the columns of a pandas DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame to rename the columns of.

    Returns
    -------
    df : pd.DataFrame
        The DataFrame with renamed columns.
    """
    renamed_df = df.copy()
    renamed_df = renamed_df.rename(columns=AIR_POLLUTION_COLUMN_MAP)
    return renamed_df


def remove_df_columns(df: pd.DataFrame, columns: List[str] = None) -> pd.DataFrame:
    """
    Remove specified columns from a pandas DataFrame.

    This function takes a DataFrame and a list of column names to remove. If no columns
    are specified, the DataFrame is returned unchanged.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame to clean and transform.
    columns : list of str or None, optional
        The columns to drop from the DataFrame. If None, no columns are dropped.
        Default is None.

    Returns
    -------
    df : pd.DataFrame
        The cleaned and transformed DataFrame.
    """
    result_df = df.copy()
    if columns:
        result_df.drop(columns=columns, inplace=True)
    return result_df


def convert_date_time_columns(df: pd.DataFrame, columns: List[str] = None) -> pd.DataFrame:
    """
    Convert Unix timestamps in specified columns to datetime format.

    This function takes a DataFrame and a list of column names, converting Unix timestamp
    values in those columns to pandas datetime objects.

    Args:
        df (pd.DataFrame): Input DataFrame containing timestamp columns to convert
        columns (list, optional): List of column names to convert. Defaults to [None]

    Returns:
        pd.DataFrame: DataFrame with specified columns converted to datetime format
    """
    result_df = df.copy()
    for col in columns:
        result_df[col] = pd.to_datetime(result_df[col], unit='s')

    return result_df


def add_audit_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add created_at and updated_at columns to DataFrame with current UTC timestamp.

    Args:
        df (pd.DataFrame): Input DataFrame

    Returns:
        pd.DataFrame: DataFrame with added audit columns
    """
    result_df = df.copy()
    current_utc = datetime.now(UTC)

    result_df['created_at'] = current_utc
    result_df['updated_at'] = current_utc

    return result_df


def convert_columns_to_float(df: pd.DataFrame, columns: List[str]):
    """
    Converts specified columns in a DataFrame to float data type.
    Args:
        df (pd.DataFrame): Input DataFrame containing columns to convert.
        columns (List[str]): List of column names to convert to float type.
    Returns:
        pd.DataFrame: A new DataFrame with specified columns converted to float type.
                     Original DataFrame remains unchanged.
    Examples:
        >>> df = pd.DataFrame({'col1': ['0', '2.3'], 'col2': ['3.4', '4.5']})
        >>> convert_columns_to_float(df, ['col1', 'col2'])
           col1  col2
        0  0.0   3.4
        1  2.3   4.5
    """
    result_df = df.copy()
    if columns:
        for col in columns:
            if col in df.columns:
                result_df[col] = result_df[col].astype(float)
    return result_df


def lowercase_df_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Return a DataFrame with lowercase column names."""
    df.columns = df.columns.str.lower()
    return df
