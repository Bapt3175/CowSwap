import pandas as pd
import logging
import yaml
from datetime import datetime, timezone
from dateutil import parser
from typing import List, Dict, Any, Optional


def load_config(logger: logging, config_path: str = "config.yml") -> Dict[str, Any]:
    try:
        with open(config_path, "r") as file:
            return yaml.safe_load(file)
    except Exception as e:
        logger.exception(f"Failed to load config: {e}")
        raise


def setup_logging() -> logging:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
    logger = logging.getLogger()
    return logger


def convert_to_unix_timestamp(date_str: str) -> Optional[float]:
    """
    Converts a date string to a UNIX timestamp in seconds.

    Args:
        date_str (str): The date string to convert, formatted as '%Y-%m-%d %H:%M:%S.%f UTC'.

    Returns:
        int: The UNIX timestamp in seconds if conversion is successful, otherwise None.
    """
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S.%f UTC")
        unix_timestamp = int(dt.replace(tzinfo=timezone.utc).timestamp())
        return unix_timestamp
    except ValueError as e:
        logging.error(
            f"Error converting date string to UNIX timestamp: {date_str}, error: {e}"
        )
        return None


def remove_nan_price_improvement(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes rows from a DataFrame where the 'price_improvement' column contains NaN values.

    Args:
        df (pd.DataFrame): The DataFrame to process.

    Returns:
        pd.DataFrame: The DataFrame with rows containing NaN in the 'price_improvement' column removed.
    """
    df_cleaned = df.dropna(subset=["price_improvement"])
    return df_cleaned


def generate_batch_id(
    trade_data_list: List[Dict[str, any]], block_time_key: str = "block_time"
) -> int:
    """
    Generates a batch ID from the block time and assigns it to each entry in the trade data list.

    Args:
        trade_data_list (List[Dict[str, any]]): A list of dictionaries where each dictionary represents a row of trade data.
        block_time_key (str): The key in the trade data dictionary that contains the block time. Default is 'block_time'.

    Returns:
        int: The generated batch ID in the format YYYYMMDD.
    """
    if not trade_data_list or block_time_key not in trade_data_list[0]:
        raise ValueError(f"Invalid trade data list or missing '{block_time_key}' key.")

    block_time = parser.parse(trade_data_list[0][block_time_key])
    batch_id = int(block_time.strftime("%Y%m%d"))

    for trade_data in trade_data_list:
        trade_data["batch_id"] = batch_id

    return batch_id
