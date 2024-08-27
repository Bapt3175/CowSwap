import pandas as pd
from unittest.mock import patch
from cow_swap.apis.dune_fetcher import DuneDataFetcher
from cow_swap.utils import convert_to_unix_timestamp


def test_get_query_results_as_dataframe_success():
    mock_rows = [
        {"block_time": "2023-08-21 12:34:56.789 UTC", "value": 100},
        {"block_time": "2023-08-22 13:34:56.789 UTC", "value": 200},
    ]

    with patch("cow_swap.apis.dune_fetcher.DuneClient") as mock_dune_client:
        mock_dune_instance = mock_dune_client.return_value
        mock_dune_instance.get_latest_result.return_value.result.rows = mock_rows

        expected_block_timestamps = [
            convert_to_unix_timestamp("2023-08-21 12:34:56.789 UTC"),
            convert_to_unix_timestamp("2023-08-22 13:34:56.789 UTC"),
        ]

        fetcher = DuneDataFetcher(api_key="test_api_key")
        df, block_times = fetcher.get_query_results_as_dataframe(query_id=123)

        expected_df = pd.DataFrame(
            {
                "block_time": [
                    "2023-08-21 12:34:56.789 UTC",
                    "2023-08-22 13:34:56.789 UTC",
                ],
                "value": [100, 200],
                "block_timestamp": expected_block_timestamps,
            }
        )

        pd.testing.assert_frame_equal(df, expected_df)
        assert block_times == (
            expected_block_timestamps[0],
            expected_block_timestamps[1],
        )


def test_get_query_results_as_dataframe_no_results():
    with patch("cow_swap.apis.dune_fetcher.DuneClient") as mock_dune_client:
        mock_dune_instance = mock_dune_client.return_value
        mock_dune_instance.get_latest_result.return_value.result = None

        fetcher = DuneDataFetcher(api_key="test_api_key")
        df, block_times = fetcher.get_query_results_as_dataframe(query_id=123)

        assert df is None
        assert block_times is None


def test_get_query_results_as_dataframe_empty_results():
    with patch("cow_swap.apis.dune_fetcher.DuneClient") as mock_dune_client:
        mock_dune_instance = mock_dune_client.return_value
        mock_dune_instance.get_latest_result.return_value.result.rows = []

        fetcher = DuneDataFetcher(api_key="test_api_key")
        df, block_times = fetcher.get_query_results_as_dataframe(query_id=123)

        assert df is None
        assert block_times is None
