import pandas as pd
from unittest.mock import patch, MagicMock
from cow_swap.apis.api_client import CoinGeckoClient

mock_response_data = {
    "prices": [
        [1625097600000, 2000.0],
        [1625184000000, 2100.0],
    ]
}


def test_get_historical_prices_success():
    client = CoinGeckoClient()

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_response_data
        mock_get.return_value = mock_response

        min_block_time = 1625097600
        max_block_time = 1625184000

        result = client.get_historical_prices(min_block_time, max_block_time)

        expected_df = pd.DataFrame(
            {"block_timestamp": [1625097600, 1625184000], "price": [2000.0, 2100.0]}
        )

        pd.testing.assert_frame_equal(result, expected_df)


def test_get_historical_prices_unsupported_token_combination():
    client = CoinGeckoClient()

    result = client.get_historical_prices(
        1625097600, 1625184000, sell_token="btc", buy_token="eth"
    )

    assert result is None


def test_get_historical_prices_api_failure():
    client = CoinGeckoClient()

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response

        result = client.get_historical_prices(1625097600, 1625184000)

        assert result is None


def test_get_historical_prices_exception_handling():
    client = CoinGeckoClient()

    with patch("requests.get", side_effect=Exception("Test exception")):
        result = client.get_historical_prices(1625097600, 1625184000)

        assert result is None
