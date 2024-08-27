import psycopg2
from psycopg2 import extras
import logging
from typing import Callable, List, Dict, Any, Optional


class PostgreSQLProvider:
    def __init__(
        self,
        dbname: str,
        user: str,
        password: str,
        host: str,
        port: int,
        batch_size: int,
    ) -> None:
        """
        Initializes the PostgreSQLProvider with database connection parameters and batch size.

        Args:
            dbname (str): The name of the PostgreSQL database.
            user (str): The username for the PostgreSQL database.
            password (str): The password for the PostgreSQL database.
            host (str): The hostname or IP address of the PostgreSQL database.
            port (int): The port number of the PostgreSQL database.
            batch_size (int): The size of batches for batch operations.
        """
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.batch_size = batch_size

    def _get_connection(self) -> psycopg2.extensions.connection:
        """
        Establishes a connection to the PostgreSQL database.

        Returns:
            psycopg2.extensions.connection: A connection object for the PostgreSQL database.
        """
        return psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
        )

    def execute(self, operation: Callable[[psycopg2.extensions.cursor], None]) -> None:
        """
        Manages the connection and executes a provided database operation.

        Args:
            operation (Callable[[psycopg2.extensions.cursor], None]): A function that accepts a cursor and performs the database operation.
        """
        connection: Optional[psycopg2.extensions.connection] = None
        try:
            with self._get_connection() as connection:
                connection.autocommit = True
                with connection.cursor() as cursor:
                    operation(cursor)
        except psycopg2.Error as e:
            logging.error(f"Database error: {e}")
        finally:
            if connection:
                connection.close()

    def create_table_cow_swap_if_not_exists(self) -> None:
        """
        Creates the cow_swap_trades table if it doesn't exist.
        """

        def operation(cursor: psycopg2.extensions.cursor) -> None:
            create_table_query = """
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
            """
            cursor.execute(create_table_query)
            logging.info("Table cow_swap_trades created or verified successfully.")

        self.execute(operation)

    def create_table_for_average_improvement(self) -> None:
        """
        Creates the batch_improvements table if it doesn't exist, with batch_id as the primary key.
        """

        def operation(cursor: psycopg2.extensions.cursor) -> None:
            create_table_query = """
            CREATE TABLE IF NOT EXISTS batch_improvements (
                batch_id BIGINT PRIMARY KEY,
                average_improvement NUMERIC(18, 8) NOT NULL
            );
            """
            cursor.execute(create_table_query)
            logging.info("Table batch_improvements created or verified successfully.")

        self.execute(operation)

    def insert_trade_data_batch(self, trade_data_list: List[Dict[str, Any]]) -> None:
        """
        Inserts a batch of trade data into the cow_swap_trades table.

        Args:
            trade_data_list (List[Dict[str, Any]]): A list of dictionaries, each representing a row of trade data.
        """

        def operation(cursor: psycopg2.extensions.cursor) -> None:
            insert_query = """
            INSERT INTO cow_swap_trades (
                batch_id, block_number, block_time, buy_price, buy_token, sell_price, sell_token, 
                sell_token_address, token_pair, units_sold, block_timestamp, price, trade_price, price_improvement
            ) VALUES (
                %(batch_id)s, %(block_number)s, %(block_time)s, %(buy_price)s, %(buy_token)s, %(sell_price)s, %(sell_token)s, 
                %(sell_token_address)s, %(token_pair)s, %(units_sold)s, %(block_timestamp)s, %(price)s, %(trade_price)s, %(price_improvement)s
            )
            ON CONFLICT (batch_id, block_number) DO NOTHING;
            """
            extras.execute_batch(
                cursor, insert_query, trade_data_list, page_size=self.batch_size
            )
            logging.info(
                f"Batch insert of {len(trade_data_list)} rows completed successfully."
            )

        self.execute(operation)

    def insert_batch_improvement(
        self, batch_id: int, average_improvement: float
    ) -> None:
        """
        Inserts a batch improvement record into the batch_improvements table.

        Args:
            batch_id (int): The ID of the batch.
            average_improvement (float): The calculated average price improvement for the batch.
        """

        def operation(cursor: psycopg2.extensions.cursor) -> None:
            insert_query = """
            INSERT INTO batch_improvements (batch_id, average_improvement)
            VALUES (%s, %s)
            ON CONFLICT (batch_id) DO UPDATE 
            SET average_improvement = EXCLUDED.average_improvement;
            """
            cursor.execute(insert_query, (batch_id, average_improvement))
            logging.info(
                f"Inserted/Updated batch_improvement for batch_id {batch_id} with average_improvement {average_improvement}."
            )

        self.execute(operation)

    def truncate_table(self) -> None:
        """
        Truncates the cow_swap_trades table, removing all data.
        """

        def operation(cursor: psycopg2.extensions.cursor) -> None:
            truncate_table_query = "TRUNCATE TABLE cow_swap_trades;"
            cursor.execute(truncate_table_query)
            logging.info("Table cow_swap_trades truncated successfully.")

        self.execute(operation)
