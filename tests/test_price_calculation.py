import pytest
import pandas as pd
import math
from cow_swap.price_calculation import (
    match_prices_with_trades,
    calculate_trade_price,
    calculate_price_improvement,
    calculate_average_price_improvement,
)


def test_match_prices_with_trades():
    trades_data = {
        "block_timestamp": [1672444800, 1672444900, 1672445000],
        "other_trade_data": [10, 20, 30],
    }
    price_data = {"block_timestamp": [1672444750, 1672444850], "price": [100, 200]}
    trades_df = pd.DataFrame(trades_data)
    price_df = pd.DataFrame(price_data)

    merged_df = match_prices_with_trades(trades_df, price_df)

    expected_data = {
        "block_timestamp": [1672444800, 1672444900, 1672445000],
        "other_trade_data": [10, 20, 30],
        "price": [100, 200, 200],
    }
    expected_df = pd.DataFrame(expected_data)

    pd.testing.assert_frame_equal(merged_df, expected_df)


def test_calculate_trade_price():
    row_weth_buy = pd.Series(
        {"buy_token": "WETH", "sell_token": "USDC", "buy_price": 100, "sell_price": 200}
    )
    row_usdc_sell = pd.Series(
        {
            "buy_token": "USDC",
            "sell_token": "WETH",
            "buy_price": 0.005,
            "sell_price": 200,
        }
    )

    assert calculate_trade_price(row_weth_buy) == 100
    assert calculate_trade_price(row_usdc_sell) == 200


def test_calculate_price_improvement():
    matched_data = {
        "block_timestamp": [1672444800, 1672444900],
        "buy_token": ["WETH", "USDC"],
        "sell_token": ["USDC", "WETH"],
        "buy_price": [100, 0.005],
        "sell_price": [200, 200],
        "price": [150, 180],
    }
    matched_df = pd.DataFrame(matched_data)

    result_df = calculate_price_improvement(matched_df)

    expected_data = {
        "block_timestamp": [1672444800, 1672444900],
        "buy_token": ["WETH", "USDC"],
        "sell_token": ["USDC", "WETH"],
        "buy_price": [100, 0.005],
        "sell_price": [200, 200],
        "price": [150, 180],
        "trade_price": [100.0, 200.0],
        "price_improvement": [-50.0, 20.0],
    }
    expected_df = pd.DataFrame(expected_data)

    pd.testing.assert_frame_equal(result_df, expected_df)


def test_calculate_average_price_improvement():
    df = pd.DataFrame({"price_improvement": [-50, 20, 30]})
    average_improvement = calculate_average_price_improvement(df)
    assert average_improvement == pytest.approx(0.0)

    empty_df = pd.DataFrame(columns=["price_improvement"])
    result = calculate_average_price_improvement(empty_df)
    assert math.isnan(result)

    nan_df = pd.DataFrame({"price_improvement": [float("nan"), float("nan")]})
    result_nan = calculate_average_price_improvement(nan_df)
    assert math.isnan(result_nan)
