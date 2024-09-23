import mysql.connector
from mysql.connector import pooling, Error as MySQLError
from .base_database import BaseDatabase, DatabaseConnectionError
import logging
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class MySQLClient(BaseDatabase):
    """MySQL database connection and operations."""

    def __init__(self, config):
        """
        Initialize the MySQLClient class.

        Args:
            config (dict): Configuration parameters for the MySQL connection.
        """
        super().__init__(config)
        self.connection_pool = None

    def connect(self, pool_size=10):
        """Establish the MySQL database connection pool."""
        try:
            self.connection_pool = mysql.connector.pooling.MySQLConnectionPool(
                pool_name="mypool",
                pool_size=pool_size,  # Pool size is now configurable
                **self.config
            )
            if self.connection_pool:
                logger.info(f"MySQL connection pool created successfully with pool size {pool_size}.")
        except MySQLError as err:
            logger.error(f"Error creating MySQL connection pool: {err}")
            raise DatabaseConnectionError(err)

    def disconnect(self):
        """Close the MySQL database connection pool."""
        if self.connection_pool:
            self.connection_pool = None  # Effectively reset the pool since no direct close method exists
            logger.info("MySQL connection pool closed successfully.")
        else:
            logger.warning("No active connection pool to close.")

    @contextmanager
    def get_connection(self):
        """Context manager for getting a connection from the pool."""
        if not self.connection_pool:
            raise DatabaseConnectionError("Connection pool is not initialized.")

        connection = self.connection_pool.get_connection()
        try:
            yield connection
        except MySQLError as err:
            logger.error(f"Error with MySQL connection: {err}")
            raise
        finally:
            if connection and connection.is_connected():
                connection.close()
                logger.debug("MySQL connection returned to the pool.")

    def execute_query(self, query, params=None):
        """
        Execute a MySQL database query.

        Args:
            query (str): The query to be executed.
            params (tuple, optional): Parameters for the query.

        Returns:
            list: Result of the query execution if it is a SELECT query.
            int: Number of affected rows for other queries.

        Raises:
            DatabaseConnectionError: If there is an error executing the query.
        """
        with self.get_connection() as connection:
            try:
                with connection.cursor() as cursor:
                    logger.debug(f"Executing query: {query}, Params: {params}")
                    cursor.execute(query, params)

                    if query.strip().lower().startswith("select"):
                        result = cursor.fetchall()
                    else:
                        result = cursor.rowcount
                        connection.commit()

                    logger.info(f"Query executed successfully: {query}")
                    return result

            except MySQLError as err:
                if not query.strip().lower().startswith("select"):
                    connection.rollback()
                    logger.warning(f"Transaction rollback due to query error: {err}")

                logger.error(f"Error executing query: {query}, Params: {params}, Error: {err}")
                raise DatabaseConnectionError(err)

    def execute_batch_query(self, query, values):
        """
        Execute a batch of MySQL database queries.

        Args:
            query (str): The query to be executed.
            values (list of tuple): List of tuples with parameters for each query execution.

        Raises:
            DatabaseConnectionError: If there is an error executing the queries.
        """
        with self.get_connection() as connection:
            try:
                with connection.cursor() as cursor:
                    logger.debug(f"Executing batch query: {query}, First 5 values: {values[:5]}")  # Log a sample of values
                    cursor.executemany(query, values)
                    connection.commit()
                    logger.info("Batch query executed successfully.")
            except MySQLError as err:
                connection.rollback()
                logger.error(f"Error executing batch query: {query}, Error: {err}")
                raise DatabaseConnectionError(err)

    def execute_transaction(self, queries):
        """
        Execute a series of queries as a transaction.

        Args:
            queries (list of tuple): List of (query, params) tuples.

        Raises:
            DatabaseConnectionError: If there is an error executing the transaction.
        """
        with self.get_connection() as connection:
            try:
                with connection.cursor() as cursor:
                    for query, params in queries:
                        logger.debug(f"Executing transactional query: {query}, Params: {params}")
                        cursor.execute(query, params)
                    connection.commit()
                    logger.info("Transaction committed successfully.")
            except MySQLError as err:
                connection.rollback()
                logger.error(f"Error executing transaction, performing rollback. Error: {err}")
                raise DatabaseConnectionError(err)
