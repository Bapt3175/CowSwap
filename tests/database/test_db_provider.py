import pytest
from unittest.mock import patch, MagicMock
from psycopg2 import extensions
from cow_swap.database.db_provider import PostgreSQLProvider


@pytest.fixture
def mock_connection():
    with patch(
        "cow_swap.database.db_provider.psycopg2.connect", autospec=True
    ) as mock_connect:
        mock_conn = MagicMock(spec=extensions.connection)

        mock_cursor = MagicMock(spec=extensions.cursor)

        mock_cursor.mogrify.return_value = b"MOCKED SQL STATEMENT"

        mock_cursor.__enter__.return_value = mock_cursor
        mock_cursor.__exit__.return_value = None

        mock_conn.__enter__.return_value = mock_conn
        mock_conn.__exit__.return_value = None

        mock_conn.cursor.return_value = mock_cursor

        mock_connect.return_value = mock_conn

        yield mock_conn, mock_cursor


def test_create_table_cow_swap_if_not_exists(mock_connection):
    mock_conn, mock_cursor = mock_connection
    provider = PostgreSQLProvider(
        dbname="test_db",
        user="user",
        password="pass",
        host="localhost",
        port=5432,
        batch_size=100,
    )
    provider.create_table_cow_swap_if_not_exists()
    mock_cursor.execute.assert_called_once_with("""
            CREATE TABLE IF NOT EXISTS cow_swap_trades (
                batch_id BIGINT NOT NULL,
                block_number BIGINT NOT NULL,
                block_time TIMESTAMP WITH TIME ZONE NOT NULL,
                buy_price NUMERIC(18, 8),
                buy_token VARCHAR(10) NOT NULL,
                sell_price NUMERIC(18, 8),
                sell_token VARCHAR(10) NOT NULL,
                sell_token_address VARCHAR(50),
                token_pair VARCHAR(20) NOT NULL,
                units_sold NUMERIC(30, 10) NOT NULL,
                block_timestamp BIGINT NOT NULL,
                price NUMERIC(18, 8),
                trade_price NUMERIC(18, 8),
                price_improvement NUMERIC(18, 8),
                PRIMARY KEY (batch_id, block_number)
            );
            """)


def test_create_table_for_average_improvement(mock_connection):
    mock_conn, mock_cursor = mock_connection
    provider = PostgreSQLProvider(
        dbname="test_db",
        user="user",
        password="pass",
        host="localhost",
        port=5432,
        batch_size=100,
    )

    provider.create_table_for_average_improvement()

    mock_cursor.execute.assert_called_once_with("""
            CREATE TABLE IF NOT EXISTS batch_improvements (
                batch_id BIGINT PRIMARY KEY,
                average_improvement NUMERIC(18, 8) NOT NULL
            );
            """)


def test_insert_trade_data_batch(mock_connection):
    mock_conn, mock_cursor = mock_connection
    provider = PostgreSQLProvider(
        dbname="test_db",
        user="user",
        password="pass",
        host="localhost",
        port=5432,
        batch_size=100,
    )
    trade_data_list = [
        {
            "batch_id": 1,
            "block_number": 123,
            "block_time": "2023-01-01 00:00:00+00",
            "buy_price": 100.0,
            "buy_token": "WETH",
            "sell_price": 200.0,
            "sell_token": "USDC",
            "sell_token_address": "0xAddress",
            "token_pair": "WETH/USDC",
            "units_sold": 1.0,
            "block_timestamp": 1672444800,
            "price": 100.0,
            "trade_price": 105.0,
            "price_improvement": 5.0,
        }
    ]

    provider.insert_trade_data_batch(trade_data_list)
    mock_cursor.execute.assert_called_once()


def test_insert_batch_improvement(mock_connection):
    mock_conn, mock_cursor = mock_connection
    provider = PostgreSQLProvider(
        dbname="test_db",
        user="user",
        password="pass",
        host="localhost",
        port=5432,
        batch_size=100,
    )
    provider.insert_batch_improvement(batch_id=1, average_improvement=5.0)
    mock_cursor.execute.assert_called_once_with(
        """
            INSERT INTO batch_improvements (batch_id, average_improvement)
            VALUES (%s, %s)
            ON CONFLICT (batch_id) DO UPDATE 
            SET average_improvement = EXCLUDED.average_improvement;
            """,
        (1, 5.0),
    )


def test_truncate_table(mock_connection):
    mock_conn, mock_cursor = mock_connection
    provider = PostgreSQLProvider(
        dbname="test_db",
        user="user",
        password="pass",
        host="localhost",
        port=5432,
        batch_size=100,
    )
    provider.truncate_table()
    mock_cursor.execute.assert_called_once_with("TRUNCATE TABLE cow_swap_trades;")
