from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from cow_swap.utils import generate_batch_id
from cow_swap.processor import (
    Processor,
    NoTradesException,
    NoPricesException,
    NoMatchedException,
)


@pytest.fixture
def mock_dune_fetcher():
    return MagicMock()


@pytest.fixture
def mock_coingecko_client():
    return MagicMock()


@pytest.fixture
def mock_pgsql_provider():
    return MagicMock()


@pytest.fixture
def mock_logger():
    return MagicMock()


@pytest.fixture
def mock_config():
    return {"dune_api": {"query_id": 123}}


@pytest.fixture
def processor(
    mock_dune_fetcher,
    mock_coingecko_client,
    mock_pgsql_provider,
    mock_config,
    mock_logger,
):
    return Processor(
        mock_dune_fetcher,
        mock_coingecko_client,
        mock_pgsql_provider,
        mock_config,
        mock_logger,
    )


def test_fetch_and_process_trades_success(processor, mock_dune_fetcher):
    mock_trades_df = pd.DataFrame(
        {
            "buy_token": ["weth", "usdc"],
            "sell_token": ["usdc", "weth"],
            "block_time": ["2021-01-01 00:00:00", "2021-01-01 00:01:00"],
        }
    )
    mock_dune_fetcher.get_query_results_as_dataframe.return_value = (
        mock_trades_df,
        (1609459200, 1609459260),
    )

    trades_df, min_block_time, max_block_time = processor.fetch_and_process_trades(123)
    assert not trades_df.empty
    assert min_block_time == 1609459200
    assert max_block_time == 1609459260


def test_fetch_and_process_trades_no_trades(processor, mock_dune_fetcher):
    mock_dune_fetcher.get_query_results_as_dataframe.return_value = (None, None)

    with pytest.raises(NoTradesException):
        processor.fetch_and_process_trades(123)


def test_fetch_historical_prices_success(processor, mock_coingecko_client):
    mock_price_df = pd.DataFrame(
        {"block_timestamp": [1609459200, 1609459260], "price": [100.0, 101.0]}
    )
    mock_coingecko_client.get_historical_prices.return_value = mock_price_df

    price_df = processor.fetch_historical_prices(1609459200, 1609459260)
    assert not price_df.empty


def test_fetch_historical_prices_no_prices(processor, mock_coingecko_client):
    mock_coingecko_client.get_historical_prices.return_value = None

    with pytest.raises(NoPricesException):
        processor.fetch_historical_prices(1609459200, 1609459260)


def test_match_and_process_data_success(processor):
    mock_trades_df = pd.DataFrame(
        {
            "buy_token": ["weth", "usdc"],
            "sell_token": ["usdc", "weth"],
            "block_timestamp": [1609459200, 1609459260],
            "buy_price": [100.0, 0.005],
            "sell_price": [200.0, 200.0],
        }
    )
    mock_price_df = pd.DataFrame(
        {"block_timestamp": [1609459200, 1609459260], "price": [100.0, 101.0]}
    )

    with patch(
        "cow_swap.price_calculation.match_prices_with_trades",
        return_value=mock_trades_df,
    ), patch(
        "cow_swap.price_calculation.calculate_price_improvement",
        return_value=mock_trades_df,
    ), patch(
        "cow_swap.utils.remove_nan_price_improvement", return_value=mock_trades_df
    ):
        matched_df = processor.match_and_process_data(mock_trades_df, mock_price_df)
        assert not matched_df.empty


def test_match_and_process_data_no_match(processor):
    mock_trades_df = pd.DataFrame(
        {
            "buy_token": ["weth", "usdc"],
            "sell_token": ["usdc", "weth"],
            "block_timestamp": [1609459200, 1609459260],
            "buy_price": [100.0, 0.005],
            "sell_price": [200.0, 200.0],
        }
    )
    mock_price_df = pd.DataFrame(
        {"block_timestamp": [1609459200, 1609459260], "price": [100.0, 101.0]}
    )

    with patch(
        "cow_swap.processor.match_prices_with_trades", return_value=None
    ) as mock_match:
        print(f"mock_match called: {mock_match.called}")
        try:
            processor.match_and_process_data(mock_trades_df, mock_price_df)
            assert False, "Expected NoMatchedException but none was raised."
        except NoMatchedException:
            pass


def save_to_database(
    self, matched_df: pd.DataFrame, average_improvement: float
) -> None:
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
