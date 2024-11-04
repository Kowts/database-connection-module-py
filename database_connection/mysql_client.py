import mysql.connector
from mysql.connector import pooling, Error as MySQLError
from .base_database import BaseDatabase, DatabaseConnectionError
from utils import retry
import logging
from contextlib import contextmanager
import time
from typing import Any, Dict, List, Optional
import json
import queue

# Create a queue to hold failed queries
failed_query_queue = queue.Queue()
MAX_RETRIES = 3

logger = logging.getLogger(__name__)

class MySQLClient(BaseDatabase):
    """MySQL database connection and operations with enhanced functionality."""

    def __init__(self, config):
        """
        Initialize the MySQLClient class.

        Args:
            config (dict): Configuration parameters for the MySQL connection.
        """
        super().__init__(config)
        self.connection_pool = None

    def connect(self):
        """Establish the MySQL database connection pool with enhanced configuration."""
        try:
            pool_config = {
                'pool_name': "mypool",
                'pool_size': self.config.get('max_pool_size', 10),
                'pool_reset_session': True,
                'connect_timeout': 30,
                **self.config
            }

            self.connection_pool = mysql.connector.pooling.MySQLConnectionPool(**pool_config)

            if self.connection_pool:
                logger.info("MySQL connection pool created successfully.")

                # Test the connection
                with self.get_connection() as conn:
                    conn.ping(reconnect=True)

        except MySQLError as err:
            logger.error(f"Error creating MySQL connection pool: {err}")
            raise DatabaseConnectionError(err)

    def disconnect(self):
        """Close the MySQL database connection pool."""
        if self.connection_pool:
            # Close all connections in the pool
            for conn in self.connection_pool._connection_queue:
                if conn and conn.is_connected():
                    conn.close()
            self.connection_pool = None
            logger.info("MySQL connection pool closed successfully.")

    @contextmanager
    def get_connection(self):
        """Context manager for getting a connection from the pool with enhanced error handling."""
        connection = None
        try:
            connection = self.connection_pool.get_connection()
            # Ensure the connection is alive
            connection.ping(reconnect=True)
            yield connection
        except MySQLError as err:
            logger.error(f"Error getting connection from pool: {err}")
            raise DatabaseConnectionError(err)
        finally:
            if connection:
                try:
                    connection.close()
                except MySQLError:
                    pass

    @retry(max_retries=5, delay=10, backoff=2, exceptions=(MySQLError,), logger=logger)
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None,
                     fetch_as_dict: bool = False, timeout: Optional[int] = None) -> Any:
        """
        Execute a MySQL database query with retry and timeout handling.

        Args:
            query (str): The query to be executed.
            params (dict or tuple, optional): Parameters for the query.
            fetch_as_dict (bool, optional): Whether to fetch results as dictionaries.
            timeout (int, optional): Query-specific timeout in seconds.

        Returns:
            Result of the query execution or number of affected rows.
        """
        start_time = time.time()

        with self.get_connection() as connection:
            try:
                cursor = connection.cursor(dictionary=fetch_as_dict)

                if timeout:
                    connection.cmd_query(f"SET SESSION MAX_EXECUTION_TIME={timeout * 1000}")

                cursor.execute(query, params or ())

                if query.strip().lower().startswith("select"):
                    result = cursor.fetchall()
                else:
                    result = cursor.rowcount
                    connection.commit()

                return result

            except MySQLError as err:
                connection.rollback()
                logger.error(f"Error executing query: {err}")
                # Log the failed query for retry
                self.log_failed_query(query, params)
                raise DatabaseConnectionError(err)

            finally:
                elapsed_time = time.time() - start_time
                if elapsed_time > self.config.get('long_query_threshold', 60):
                    logger.warning(f"Query took too long ({elapsed_time} seconds).")

    def execute_batch_query(self, query: str, values: List[tuple]):
        """
        Execute a batch of MySQL database queries with optimized performance.

        Args:
            query (str): The query to be executed.
            values (list of tuple): List of tuples with parameters for each query execution.
        """
        with self.get_connection() as connection:
            try:
                cursor = connection.cursor()
                # Use executemany with batch processing
                batch_size = 1000
                for i in range(0, len(values), batch_size):
                    batch = values[i:i + batch_size]
                    cursor.executemany(query, batch)
                connection.commit()
                logger.info("Batch query executed successfully.")
            except MySQLError as err:
                connection.rollback()
                logger.error(f"Error executing batch query: {err}")
                raise DatabaseConnectionError(err)

    def execute_transaction(self, queries: List[tuple]):
        """
        Execute a series of queries as a transaction with enhanced error handling.

        Args:
            queries (list of tuple): List of (query, params) tuples.
        """
        with self.get_connection() as connection:
            try:
                cursor = connection.cursor()
                connection.start_transaction()

                for query, params in queries:
                    cursor.execute(query, params)

                connection.commit()
                logger.info("Transaction committed successfully.")
            except MySQLError as err:
                connection.rollback()
                logger.error(f"Error executing transaction: {err}")
                raise DatabaseConnectionError(err)

    def log_failed_query(self, query: str, params: Optional[Dict[str, Any]] = None):
        """
        Log failed queries for future retry attempts.

        Args:
            query (str): The failed query.
            params (dict or tuple, optional): Parameters for the query.
        """
        try:
            failed_query_data = {
                'query': query,
                'params': json.dumps(params or {}),
                'retry_count': 0,
                'timestamp': time.time()
            }
            failed_query_queue.put(failed_query_data)
            logger.info("Failed query logged for retry.")
        except Exception as err:
            logger.error(f"Failed to log failed query: {err}")
            with open('failed_queries.log', 'a') as f:
                f.write(f"{query} | {params}\n")

    def process_failed_queries(self):
        """Process and retry failed queries from the queue."""
        while not failed_query_queue.empty():
            failed_query = failed_query_queue.get()

            if failed_query['retry_count'] >= MAX_RETRIES:
                logger.warning(f"Max retries reached for query. Skipping.")
                continue

            try:
                query = failed_query['query']
                params = json.loads(failed_query['params'])

                self.execute_query(query, params)
                logger.info("Successfully retried failed query.")

            except MySQLError as err:
                failed_query['retry_count'] += 1
                failed_query_queue.put(failed_query)
                logger.error(f"Failed to retry query: {err}")

    def cleanup_old_failed_queries(self, max_age_days: int = 30):
        """
        Clean up old failed queries from the queue.

        Args:
            max_age_days (int): Maximum age in days for keeping failed queries.
        """
        current_time = time.time()
        max_age_seconds = max_age_days * 24 * 60 * 60

        new_queue = queue.Queue()

        while not failed_query_queue.empty():
            query = failed_query_queue.get()
            if current_time - query['timestamp'] < max_age_seconds:
                new_queue.put(query)

        global failed_query_queue
        failed_query_queue = new_queue

        logger.info(f"Cleaned up failed queries older than {max_age_days} days.")

    def execute_query_with_savepoint(self, query: str, params: Optional[Dict[str, Any]] = None):
        """
        Execute a query with a savepoint for better transaction control.

        Args:
            query (str): The SQL query to execute.
            params (dict, optional): Parameters for the query.
        """
        with self.get_connection() as connection:
            try:
                cursor = connection.cursor()
                connection.start_transaction()

                cursor.execute("SAVEPOINT my_savepoint")
                cursor.execute(query, params or ())
                connection.commit()

            except MySQLError as err:
                cursor.execute("ROLLBACK TO SAVEPOINT my_savepoint")
                connection.rollback()
                logger.error(f"Query failed, rolled back to savepoint: {err}")
                raise
