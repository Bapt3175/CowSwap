from typing import Tuple

import pandas as pd

from cow_swap.price_calculation import (
    match_prices_with_trades,
    calculate_price_improvement,
    calculate_average_price_improvement,
)
from cow_swap.utils import remove_nan_price_improvement, generate_batch_id


class NoTradesException(Exception):
    pass


class NoPricesException(Exception):
    pass


class NoMatchedException(Exception):
    pass


class Processor:
    def __init__(self, dune_fetcher, coingecko_client, pgsql_provider, config, logger):
        self.dune_fetcher = dune_fetcher
        self.coingecko_client = coingecko_client
        self.pgsql_provider = pgsql_provider
        self.config = config
        self.logger = logger

    def process(self):
        query_id = self.config["dune_api"]["query_id"]

        trades_df, min_block_time, max_block_time = self.fetch_and_process_trades(
            query_id
        )
        price_df = self.fetch_historical_prices(min_block_time, max_block_time)
        matched_df = self.match_and_process_data(trades_df, price_df)
        average_improvement = calculate_average_price_improvement(matched_df)
        self.save_to_database(matched_df, average_improvement)

    @staticmethod
    def filter_weth_usdc(df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter the DataFrame to only include rows where the tokens are either WETH or USDC.
        This ensures that only relevant trades involving these tokens are processed.

        Args:
            df (pd.DataFrame): A DataFrame containing trade data with 'buy_token' and 'sell_token' columns.

        Returns:
            pd.DataFrame: A filtered DataFrame containing only trades where either the 'buy_token' or 'sell_token'
                          is WETH or USDC.
        """
        return df[
            (df["buy_token"].str.lower().isin(["weth", "usdc"]))
            & (df["sell_token"].str.lower().isin(["weth", "usdc"]))
            ]

    def fetch_and_process_trades(
        self, query_id: int
    ) -> Tuple[pd.DataFrame, float, float]:
        """
        Fetches and processes trade data from a Dune Analytics query.

        Args:
            query_id (int): The ID of the Dune Analytics query.

        Returns:
            Tuple[[pd.DataFrame], [float], [float]]:
                A tuple containing the trades DataFrame, the minimum block time, and the maximum block time.
                Returns (None, None, None) if an error occurs.
        """

        trades_df, block_time_interval = (
            self.dune_fetcher.get_query_results_as_dataframe(query_id)
        )
        if trades_df is None:
            raise NoTradesException("No trades data fetched.")

        trades_df = self.filter_weth_usdc(trades_df)
        min_block_time, max_block_time = block_time_interval
        self.logger.info(f"Block time interval: {min_block_time} to {max_block_time}")

        return trades_df, min_block_time, max_block_time

    def fetch_historical_prices(
        self, min_block_time: float, max_block_time: float
    ) -> pd.DataFrame:
        """
        Fetches historical prices for a default token pair over a given time range using the CoinGeckoClient.

        Args:
            min_block_time (float): The start of the time range, in UNIX timestamp format (seconds since epoch).
            max_block_time (float): The end of the time range, in UNIX timestamp format (seconds since epoch).

        Returns:
            pd.DataFrame: A DataFrame containing the historical prices, or None if an error occurs.
        """

        price_df = self.coingecko_client.get_historical_prices(
            min_block_time, max_block_time
        )
        if price_df is None:
            raise NoPricesException("No historical prices fetched.")

        self.logger.info("Fetched historical prices done")
        return price_df

    @staticmethod
    def match_and_process_data(
        trades_df: pd.DataFrame, price_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Matches and processes trade data with historical price data.

        Args:
            trades_df (pd.DataFrame): The DataFrame containing trade data.
            price_df (pd.DataFrame): The DataFrame containing historical price data.

        Returns:
            pd.DataFrame: A DataFrame containing the matched and processed data, or None if an error occurs.
        """

        matched_df = match_prices_with_trades(trades_df, price_df)

        if matched_df is None:
            raise NoMatchedException("No matched process data")

        matched_df = calculate_price_improvement(matched_df)
        matched_df = remove_nan_price_improvement(matched_df)

        return matched_df

    def save_to_database(
        self, matched_df: pd.DataFrame, average_improvement: float
    ) -> None:
        """
        Saves the processed trade data and average price improvement to the database.

        Args:
            matched_df (pd.DataFrame): The DataFrame containing matched and processed trade data.
            average_improvement (float): The calculated average price improvement.

        Raises:
            Exception: If there is an error saving data to the database.
        """
        try:
            self.pgsql_provider.create_table_cow_swap_if_not_exists()
            self.pgsql_provider.create_table_for_average_improvement()

            trade_data_list = [row.to_dict() for _, row in matched_df.iterrows()]
            batch_id = generate_batch_id(trade_data_list)
            self.pgsql_provider.insert_trade_data_batch(trade_data_list)
            self.pgsql_provider.insert_batch_improvement(batch_id, average_improvement)
            self.logger.info("Data successfully saved to the database.")
        except Exception as e:
            self.logger.exception(f"Error saving data to the database: {e}")
