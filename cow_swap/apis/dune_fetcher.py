import pandas as pd
import logging
from dune_client.client import DuneClient
from cow_swap.utils import convert_to_unix_timestamp
from typing import Optional, Tuple


class DuneDataFetcher:
    """
    A client for interacting with the Dune Analytics API to fetch query results and convert them into a DataFrame.

    Attributes:
        dune (DuneClient): An instance of the DuneClient to interact with the Dune Analytics API.
    """

    def __init__(self, api_key: str) -> None:
        """
        Initializes the DuneDataFetcher with an API key.

        Args:
            api_key (str): The API key to authenticate with the Dune Analytics API.
        """
        self.dune = DuneClient(api_key)

    def get_query_results_as_dataframe(
        self, query_id: int
    ) -> Tuple[Optional[pd.DataFrame], Optional[Tuple[int, int]]]:
        """
        Fetches the latest results of a specified query from Dune Analytics and converts them into a DataFrame.

        Args:
            query_id (int): The ID of the query to fetch results for.

        Returns:
            Tuple[Optional[pd.DataFrame], Optional[Tuple[int, int]]]:
                - A DataFrame containing the query results with an added 'block_timestamp' column in UNIX format.
                - A tuple containing the minimum and maximum block timestamps in UNIX format.
                - Returns (None, None) if no results are found or if the query is still running.
        """
        query_result = self.dune.get_latest_result(query_id)
        if query_result.result and query_result.result.rows:
            df = pd.DataFrame(query_result.result.rows)

            if df.empty or "block_time" not in df.columns:
                logging.warning("No valid data found in query results.")
                return None, None

            df["block_timestamp"] = df["block_time"].apply(convert_to_unix_timestamp)
            min_block_time = df["block_timestamp"].min()
            max_block_time = df["block_timestamp"].max()

            return df, (min_block_time, max_block_time)
        else:
            logging.warning("No results found or query is still running.")
            return None, None
