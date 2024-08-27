import logging
import requests
import pandas as pd
from typing import Optional


class CoinGeckoClient:
    """
    A client for interacting with the CoinGecko API to fetch historical price data.

    Attributes:
        base_url (str): The base URL for the CoinGecko API.
    """

    def __init__(self) -> None:
        """
        Initializes the CoinGeckoClient with the base URL for the CoinGecko API.
        """
        self.base_url = "https://api.coingecko.com/api/v3"

    def get_historical_prices(
        self,
        min_block_time: float,
        max_block_time: float,
        sell_token: str = "weth",
        buy_token: str = "usdc",
    ) -> Optional[pd.DataFrame]:
        """
        Fetches historical prices for a specified token pair over a given time range from the CoinGecko API.

        Args:
            min_block_time (float): The start of the time range, in UNIX timestamp format (seconds since epoch).
            max_block_time (float): The end of the time range, in UNIX timestamp format (seconds since epoch).
            sell_token (str): The symbol of the sell token. Default is 'weth'.
            buy_token (str): The symbol of the buy token. Default is 'usdc'.

        Returns:
            Optional[pd.DataFrame]: A DataFrame containing the historical prices with columns 'block_timestamp' and 'price'.
            Returns None if there is an error or if the token combination is unsupported.
        """
        if buy_token.lower() == "weth" or sell_token.lower() == "weth":
            vs_currency, coin_id = "usd", "ethereum"
        elif buy_token.lower() == "usdc" or sell_token.lower() == "usdc":
            vs_currency, coin_id = "usd", "usd"
        else:
            logging.error(
                f"Unsupported token combination: sell_token={sell_token}, buy_token={buy_token}. Only WETH and USDC are supported."
            )
            return None

        url = f"{self.base_url}/coins/{coin_id}/market_chart/range"
        params = {
            "vs_currency": vs_currency,
            "from": min_block_time,
            "to": max_block_time,
        }

        try:
            response = requests.get(url, params=params)
            logging.info(f"API request URL: {response.url}")
            logging.info(f"Status Code: {response.status_code}")

            if response.status_code == 200:
                data = response.json()
                prices = data.get("prices", [])
                if prices:
                    df = pd.DataFrame(prices, columns=["block_timestamp", "price"])
                    df["block_timestamp"] = (df["block_timestamp"] / 1000).astype(int)
                    df["price"] = df["price"].apply(lambda x: round(x, 8))
                    return df
                else:
                    logging.warning("No price data found.")
                    return None
            else:
                logging.error(f"Failed to fetch data: {response.status_code}")
                return None
        except Exception as e:
            logging.error(f"Error fetching price data: {e}")
            return None
