import pandas as pd
import logging


def match_prices_with_trades(
    trades_df: pd.DataFrame, price_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Matches historical prices with trades based on the closest previous block timestamp.

    Args:
        trades_df (pd.DataFrame): DataFrame containing trade data with a 'block_timestamp' column.
        price_df (pd.DataFrame): DataFrame containing historical price data with a 'block_timestamp' column.

    Returns:
        pd.DataFrame: A DataFrame that merges the trade data with the closest matching price data.
    """
    trades_df["block_timestamp"] = trades_df["block_timestamp"].astype(int)
    merged_df = pd.merge_asof(
        trades_df.sort_values("block_timestamp"),
        price_df.sort_values("block_timestamp"),
        on="block_timestamp",
        direction="backward",
    )
    return merged_df


def calculate_trade_price(row: pd.Series) -> float:
    """
    Calculates the trade price based on the token type.

    Args:
        row (pd.Series): A row of trade data containing 'buy_token', 'sell_token', 'buy_price', and 'sell_price'.

    Returns:
        float: The calculated trade price.
    """
    if row["buy_token"].lower() == "weth":
        return row["buy_price"]
    elif row["sell_token"].lower() == "weth":
        return row["sell_price"]
    elif row["buy_token"].lower() == "usdc":
        return 1 / row["buy_price"]
    elif row["sell_token"].lower() == "usdc":
        return row["sell_price"]
    else:
        logging.error(
            f"Unexpected token combination: {row['buy_token']} / {row['sell_token']}"
        )
        return None


def calculate_price_improvement(matched_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the price improvement for each trade by comparing the trade price to the historical price.
    If the sell_token is "WETH", the price improvement is multiplied by -1.

    Args:
        matched_df (pd.DataFrame): DataFrame containing merged trade and historical price data.

    Returns:
        pd.DataFrame: The input DataFrame with additional columns for trade price and price improvement.
    """
    matched_df["trade_price"] = matched_df.apply(calculate_trade_price, axis=1)
    matched_df["price_improvement"] = matched_df["trade_price"] - matched_df["price"]

    matched_df.loc[matched_df["sell_token"].str.lower() == "weth", "price_improvement"] *= -1

    return matched_df


def calculate_average_price_improvement(
    df: pd.DataFrame, price_improvement_column: str = "price_improvement"
) -> float:
    """
    Calculates the average price improvement from a DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame containing trade data, including a column for price improvement.
        price_improvement_column (str): The name of the column containing the price improvement values. Default is 'price_improvement'.

    Returns:
        float: The average price improvement, or None if the DataFrame is empty or the column does not exist.
    """

    average_improvement = df[price_improvement_column].mean()
    logging.info(f"Average price improvement calculated: {average_improvement}")

    return float(average_improvement)
