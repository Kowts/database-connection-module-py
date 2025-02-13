import os
import sys
import oracledb
from .base_database import BaseDatabase, DatabaseConnectionError
import logging
from contextlib import contextmanager
from utils import retry_etl
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

    def initialize_oracle_client(self):
        """
        Initializes the Oracle Client depending on the OS.
        """
        try:
            # Windows: Use the Instant Client from Navicat
            if sys.platform.startswith("win"):
                if not oracledb.is_thick_mode():
                    lib_dir = r"C:\Program Files\PremiumSoft\Navicat Premium 16\instantclient_11_2"

                    if os.path.isdir(lib_dir):
                        oracledb.init_oracle_client(lib_dir=lib_dir)
                    else:
                        raise FileNotFoundError(f"Oracle Instant Client not found at: {lib_dir}")

            # Linux/macOS: Use Oracle Instant Client
            else:
                lib_dir = "/opt/oracle/instantclient"
                if os.path.isdir(lib_dir):
                    oracledb.init_oracle_client(lib_dir=lib_dir)
                else:
                    raise FileNotFoundError(f"Oracle Instant Client not found at: {lib_dir}")

            print("✅ Oracle Client initialized successfully.")

        except Exception as e:
            print(f"⚠️ Error initializing Oracle Client: {e}")
            sys.exit(1)  # Exit the script if the client cannot be initialized

    def connect(self):
        """Establish a connection pool to the Oracle database."""
        try:

            # Initialize Oracle client if necessary
            self.initialize_oracle_client()

            if 'service_name' in self.config:
                dsn = oracledb.makedsn(self.config['host'], self.config['port'], service_name=self.config['service_name'])
            elif 'sid' in self.config:
                dsn = oracledb.makedsn(self.config['host'], self.config['port'], sid=self.config['sid'])
            else:
                raise ValueError("Either 'service_name' or 'sid' must be provided in the configuration.")

            # Create a connection pool
            self.pool = oracledb.create_pool(
                user=self.config['user'],
                password=self.config['password'],
                dsn=dsn,
                min=2,
                max=10,
                increment=1,
                timeout=30,
                wait_timeout=10000,
                max_lifetime_session=28800,
                ping_interval=60
            )

            logger.info("Oracle connection pool created successfully.")
        except oracledb.Error as err:  # Updated exception handling
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
            oracledb.Connection: A connection object from the pool.
        """
        connection = None
        try:
            connection = self.pool.acquire()
            yield connection
        finally:
            if connection:
                self.pool.release(connection)

    @retry_etl(max_retries=5, delay=10, backoff=2, exceptions=(oracledb.Error,), logger=logger)
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
                # Create a cursor for the connection
                cursor = connection.cursor()
                if timeout:
                    connection.call_timeout = timeout * 1000  # Oracle uses milliseconds for timeout

                # Execute the query
                cursor.execute(query, params or {})

                # Fetch results if it is a SELECT query
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

            except oracledb.Error as err:
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
            except oracledb.Error as err:
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
            except oracledb.Error as err:
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
