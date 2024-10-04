import psycopg2
from psycopg2 import pool, OperationalError, DatabaseError
from psycopg2.extras import DictCursor, execute_batch
from .base_database import BaseDatabase, DatabaseConnectionError
import logging
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class PostgreSQLClient(BaseDatabase):
    """PostgreSQL database connection and operations."""

    def __init__(self, config):
        """
        Initialize the PostgreSQLClient class.

        Args:
            config (dict): Configuration parameters for the PostgreSQL connection.
        """
        super().__init__(config)
        self.connection_pool = None

    def connect(self):
        """Establish the PostgreSQL database connection pool."""
        if self.connection_pool:
            logger.warning("Connection pool is already initialized.")
            return
        try:
            self.connection_pool = psycopg2.pool.SimpleConnectionPool(
                minconn=1,
                maxconn=20,
                **self.config
            )
            if self.connection_pool:
                logger.info("PostgreSQL connection pool created successfully.")
        except OperationalError as err:
            logger.error(f"Error creating PostgreSQL connection pool: {err}")
            raise DatabaseConnectionError(err)

    def is_connected(self):
        """Check if the connection pool is active and connections are available."""
        if self.connection_pool is None:
            return False

        try:
            # Attempt to get a connection from the pool without reserving it
            connection = self.connection_pool.getconn()
            self.connection_pool.putconn(connection)
            return True
        except (OperationalError, DatabaseError):
            logger.error("Unable to get a connection from the pool.")
            return False

    def disconnect(self):
        """Close the PostgreSQL database connection pool."""
        if self.connection_pool:
            self.connection_pool.closeall()
            logger.info("PostgreSQL connection pool closed successfully.")
            self.connection_pool = None  # Set to None to mark it as closed
        else:
            logger.warning("Connection pool is already closed.")


    @contextmanager
    def get_connection(self):
        """Context manager for getting a connection from the pool."""
        if not self.connection_pool:
            raise DatabaseConnectionError("Connection pool is not initialized.")
        connection = self.connection_pool.getconn()
        try:
            yield connection
        except Exception as err:
            logger.error(f"Error during connection handling: {err}")
            raise
        finally:
            self.connection_pool.putconn(connection)

    def execute_query(self, query, params=None, fetch_as_dict=False):
        """
        Execute a PostgreSQL database query.

        Args:
            query (str or psycopg2.sql.SQL): The query to be executed.
            params (tuple or dict, optional): Parameters for the query.
            fetch_as_dict (bool, optional): Whether to fetch results as dictionaries.

        Returns:
            list: Result of the query execution if it is a SELECT query.
            int: Number of affected rows for other queries.

        Raises:
            DatabaseConnectionError: If there is an error executing the query.
        """
        with self.get_connection() as connection:
            try:
                with connection.cursor(cursor_factory=DictCursor if fetch_as_dict else None) as cursor:
                    logger.debug(f"Executing query: {query}, Params: {params}")
                    cursor.execute(query, params)
                    if cursor.description:  # cursor.description is None for non-SELECT queries
                        result = cursor.fetchall()
                    else:
                        result = cursor.rowcount
                        connection.commit()
                    return result
            except (OperationalError, DatabaseError) as err:
                connection.rollback()
                logger.error(f"Error executing query: {query}, Params: {params}, Error: {err}")
                raise DatabaseConnectionError(err)

    def execute_batch_query(self, query, values):
        """
        Execute a batch of PostgreSQL database queries.

        Args:
            query (str or psycopg2.sql.SQL): The query to be executed.
            values (list of tuple): List of tuples with parameters for each query execution.

        Raises:
            DatabaseConnectionError: If there is an error executing the queries.
        """
        with self.get_connection() as connection:
            try:
                with connection.cursor() as cursor:
                    logger.debug(f"Executing batch query: {query}, Values: {values[:10]}...")  # Log only a sample of values
                    execute_batch(cursor, query, values)
                    connection.commit()
                    logger.info("Batch query executed successfully.")
            except (OperationalError, DatabaseError) as err:
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
            except (OperationalError, DatabaseError) as err:
                connection.rollback()
                logger.error(f"Error executing transaction: {err}")
                raise DatabaseConnectionError(err)
