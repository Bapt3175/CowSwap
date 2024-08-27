import pytest
import pandas as pd
from datetime import datetime, timezone
from cow_swap.utils import (
    convert_to_unix_timestamp,
    remove_nan_price_improvement,
    generate_batch_id,
)


def test_convert_to_unix_timestamp():
    date_str = "2023-08-21 12:34:56.789 UTC"
    expected_timestamp = int(
        datetime(2023, 8, 21, 12, 34, 56, 789000, tzinfo=timezone.utc).timestamp()
    )
    assert convert_to_unix_timestamp(date_str) == expected_timestamp

    invalid_date_str = "invalid date"
    assert convert_to_unix_timestamp(invalid_date_str) is None

    wrong_format_str = "2023/08/21 12:34:56.789"
    assert convert_to_unix_timestamp(wrong_format_str) is None


def test_remove_nan_price_improvement():
    data = {
        "price_improvement": [1.5, None, 2.5, None],
        "other_column": [10, 20, 30, 40],
    }
    df = pd.DataFrame(data)

    cleaned_df = remove_nan_price_improvement(df)

    cleaned_df.reset_index(drop=True, inplace=True)

    expected_data = {"price_improvement": [1.5, 2.5], "other_column": [10, 30]}
    expected_df = pd.DataFrame(expected_data)

    pd.testing.assert_frame_equal(cleaned_df, expected_df)


def test_generate_batch_id():
    trade_data_list = [
        {"block_time": "2023-08-21 12:34:56.789 UTC"},
        {"block_time": "2023-08-21 13:34:56.789 UTC"},
    ]
    expected_batch_id = 20230821
    assert generate_batch_id(trade_data_list) == expected_batch_id
    assert trade_data_list[0]["batch_id"] == expected_batch_id
    assert trade_data_list[1]["batch_id"] == expected_batch_id

    with pytest.raises(ValueError):
        generate_batch_id([])

    incomplete_trade_data_list = [{"some_other_key": "2023-08-21 12:34:56.789 UTC"}]
    with pytest.raises(ValueError):
        generate_batch_id(incomplete_trade_data_list)
