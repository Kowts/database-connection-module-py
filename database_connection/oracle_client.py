import cx_Oracle
from .base_database import BaseDatabase, DatabaseConnectionError
import logging
from contextlib import contextmanager
from .utils import retry
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

class OracleClient(BaseDatabase):
    """Oracle database connection and operations with connection pooling."""

    def __init__(self, config):
        """
        Initialize the OracleClient class.

        Args:
            config (dict): Configuration parameters for the Oracle connection.
        """
        super().__init__(config)
        self.pool = None

    def connect(self):
        """Establish a connection pool to the Oracle database."""
        try:
            # Initialize Oracle client if necessary
            cx_Oracle.init_oracle_client(lib_dir=r"C:\Program Files\PremiumSoft\Navicat Premium 16\instantclient_11_2")

            # Use service_name or sid based on configuration
            if 'service_name' in self.config:
                dsn = cx_Oracle.makedsn(
                    self.config['host'], self.config['port'], service_name=self.config['service_name'])
            elif 'sid' in self.config:
                dsn = cx_Oracle.makedsn(
                    self.config['host'], self.config['port'], sid=self.config['sid'])
            else:
                raise ValueError(
                    "Either 'service_name' or 'sid' must be provided in the configuration.")

            # Create a connection pool for efficient connection management
            self.pool = cx_Oracle.SessionPool(
                user=self.config['user'],
                password=self.config['password'],
                dsn=dsn,
                min=2,  # Minimum number of connections in the pool
                max=10,  # Maximum number of connections in the pool
                increment=1,  # Increment by 1 connection when more are needed
                threaded=True,  # Allow multithreading
                encoding="UTF-8"
            )

            logger.info("Oracle connection pool created successfully.")
        except cx_Oracle.Error as err:
            error, = err.args
            logger.error(f"Error creating Oracle connection pool: {error.message}")
            raise DatabaseConnectionError(error.message)

    def disconnect(self):
        """Close the Oracle connection pool."""
        if self.pool:
            self.pool.close()
            logger.info("Oracle connection pool closed successfully.")

    @contextmanager
    def get_connection(self):
        """
        Context manager for getting a connection from the pool.

        Yields:
            cx_Oracle.Connection: A connection object from the pool.
        """
        connection = None
        try:
            connection = self.pool.acquire()
            yield connection
        finally:
            if connection:
                self.pool.release(connection)

    @retry(max_retries=5, delay=10, backoff=2, exceptions=(cx_Oracle.Error,), logger=logger)
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None, fetch_as_dict: bool = False, timeout: Optional[int] = None) -> Any:
        """
        Execute an Oracle database query with retry and timeout handling.

        Args:
            query (str): The query to be executed.
            params (dict or tuple, optional): Parameters for the query.
            fetch_as_dict (bool, optional): Whether to fetch results as dictionaries.
            timeout (int, optional): Query-specific timeout (in seconds). If not provided, uses default timeout.

        Returns:
            list: Result of the query execution if it is a SELECT query.
            int: Number of affected rows for other queries.

        Raises:
            DatabaseConnectionError: If there is an error executing the query.
        """
        start_time = time.time()
        with self.get_connection() as connection:
            try:
                cursor = connection.cursor()
                if timeout:
                    connection.callTimeout = timeout * 1000  # Oracle uses milliseconds for timeout
                cursor.execute(query, params or {})

                if query.strip().lower().startswith("select"):
                    if fetch_as_dict:
                        columns = [col[0] for col in cursor.description]
                        result = [dict(zip(columns, row)) for row in cursor.fetchall()]
                    else:
                        result = cursor.fetchall()
                else:
                    result = cursor.rowcount
                    connection.commit()
                return result

            except cx_Oracle.Error as err:
                logger.error(f"Error executing query: {err}")
                connection.rollback()
                raise DatabaseConnectionError(err)

            finally:
                elapsed_time = time.time() - start_time
                if elapsed_time > self.config.get('long_query_threshold', 60):
                    logger.warning(f"Query took too long ({elapsed_time} seconds): {query}")

    def execute_batch_query(self, query: str, values: List[tuple]):
        """
        Execute a batch of Oracle database queries.

        Args:
            query (str): The query to be executed.
            values (list of tuple): List of tuples with parameters for each query execution.

        Raises:
            DatabaseConnectionError: If there is an error executing the queries.
        """
        with self.get_connection() as connection:
            try:
                cursor = connection.cursor()
                cursor.executemany(query, values)
                connection.commit()
                logger.info("Batch query executed successfully.")
            except cx_Oracle.Error as err:
                connection.rollback()
                logger.error(f"Error executing batch query: {err}")
                raise DatabaseConnectionError(err)

    def execute_transaction(self, queries: List[tuple]):
        """
        Execute a series of queries as a transaction.

        Args:
            queries (list of tuple): List of (query, params) tuples.

        Raises:
            DatabaseConnectionError: If there is an error executing the transaction.
        """
        with self.get_connection() as connection:
            try:
                cursor = connection.cursor()
                for query, params in queries:
                    cursor.execute(query, params)
                connection.commit()
                logger.info("Transaction committed successfully.")
            except cx_Oracle.Error as err:
                connection.rollback()
                logger.error(f"Error executing transaction: {err}")
                raise DatabaseConnectionError(err)

    def log_failed_query(self, query: str, params: Optional[Dict[str, Any]] = None):
        """
        Log the failed query for future debugging.

        Args:
            query (str): The failed query.
            params (dict or tuple, optional): Parameters for the query.
        """
        try:
            logger.error(f"Failed query: {query} | Params: {params}")
            with open('failed_queries.log', 'a') as f:
                f.write(f"{query} | {params}\n")
        except Exception as err:
            logger.error(f"Failed to log query: {err}")
