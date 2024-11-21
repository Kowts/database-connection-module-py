import psycopg2
from psycopg2 import pool, OperationalError, DatabaseError
from psycopg2.extras import DictCursor, execute_batch, execute_values
from .base_database import BaseDatabase, DatabaseConnectionError
import logging
import threading
import json
import queue
import time
import re
from typing import Any, Dict, List, Optional, Tuple, Union
from contextlib import contextmanager

# Create a queue to hold failed queries
failed_query_queue = queue.Queue()
MAX_RETRIES = 3

logger = logging.getLogger(__name__)

class PostgreSQLClient(BaseDatabase):
    """PostgreSQL database connection and operations with enhanced functionality."""

    def __init__(self, config):
        """
        Initialize the PostgreSQLClient class.

        Args:
            config (dict): Configuration parameters for the PostgreSQL connection.
        """
        super().__init__(config)
        self._local = threading.local()
        self._connection_lock = threading.Lock()
        self.connection_pool = None

    def connect(self):
        """
        Establish the PostgreSQL database connection.
        Sets up connection pooling and configuration.
        """
        try:
            # Set default configuration values if not provided
            min_pool_size = self.config.get('min_pool_size', 2)
            max_pool_size = self.config.get('max_pool_size', 10)

            self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=min_pool_size,
                maxconn=max_pool_size,
                host=self.config['host'],
                port=self.config['port'],
                database=self.config['dbname'],
                user=self.config['user'],
                password=self.config['password'],
                connect_timeout=self.config.get('connection_timeout', 30),
                application_name=self.config.get('application_name', 'PostgreSQLClient'),
                options=self.config.get('options', '-c statement_timeout=30000'),  # 30 second query timeout
                **self.config.get('additional_params', {})
            )

            # Test connection
            with self._get_new_connection() as test_conn:
                logger.info("PostgreSQL Database connection pool configured successfully.")

        except OperationalError as err:
            logger.error(f"Error connecting to PostgreSQL Database: {err}")
            raise DatabaseConnectionError(err)

    @contextmanager
    def _get_new_connection(self):
        """Internal method to get a new connection."""
        connection = None
        try:
            connection = self.connection_pool.getconn()
            yield connection
        finally:
            if connection:
                try:
                    self.connection_pool.putconn(connection)
                except:
                    pass

    @contextmanager
    def get_connection(self):
        """
        Context manager for getting a connection with proper lifecycle management.
        """
        try:
            with self._connection_lock:
                if not hasattr(self._local, 'connection') or self._local.connection is None:
                    self._local.connection = self.connection_pool.getconn()

            yield self._local.connection

        except Exception as e:
            if hasattr(self._local, 'connection') and self._local.connection:
                try:
                    self._local.connection.rollback()
                    self.connection_pool.putconn(self._local.connection)
                except:
                    pass
                finally:
                    self._local.connection = None
            raise e

    def _format_query_and_params(self, query: str,
                                params: Optional[Union[Dict[str, Any], Tuple[Any], List[Any]]] = None) -> Tuple[str, Any]:
        """
        Format query and parameters for PostgreSQL.

        Args:
            query: SQL query with potential {tableDate} placeholder
            params: Query parameters

        Returns:
            Tuple of (formatted query, formatted parameters)
        """
        try:
            if params is None:
                return query, None

            # Convert %s to $1, $2, etc. for named parameters
            if isinstance(params, dict):
                if 'tableDate' in params:
                    formatted_query = query.replace("{tableDate}", str(params['tableDate']))
                    remaining_params = {k: v for k, v in params.items() if k != 'tableDate'}
                    formatted_params = tuple(remaining_params.values()) if remaining_params else None
                    return formatted_query, formatted_params
                param_count = 1
                formatted_query = query
                while "%s" in formatted_query:
                    formatted_query = formatted_query.replace("%s", f"${param_count}", 1)
                    param_count += 1
                return formatted_query, tuple(params.values())

            return query, params

        except Exception as e:
            logger.error(f"Error formatting query: {e}")
            raise ValueError(f"Query formatting failed: {e}")

    def execute_query(self, query: str,
                     params: Optional[Union[Dict[str, Any], Tuple[Any], List[Any]]] = None,
                     fetch_as_dict: bool = False,
                     timeout: Optional[int] = None) -> Any:
        """
        Execute a PostgreSQL query with enhanced error handling.

        Args:
            query: SQL query to execute
            params: Query parameters
            fetch_as_dict: Whether to return results as dictionaries
            timeout: Query timeout in seconds

        Returns:
            Query results or affected row count
        """
        start_time = time.time()

        try:
            formatted_query, formatted_params = self._format_query_and_params(query, params)

            with self.get_connection() as connection:
                cursor_factory = DictCursor if fetch_as_dict else None
                with connection.cursor(cursor_factory=cursor_factory) as cursor:
                    if timeout:
                        cursor.execute(f"SET statement_timeout = {timeout * 1000}")

                    cursor.execute(formatted_query, formatted_params or ())

                    if cursor.description:  # SELECT query
                        result = cursor.fetchall()
                        if fetch_as_dict:
                            columns = [desc[0] for desc in cursor.description]
                            result = [dict(zip(columns, row)) for row in result]
                    else:
                        result = cursor.rowcount
                        connection.commit()

                    return result

        except (OperationalError, DatabaseError) as err:
            error_msg = f"""
            PostgreSQL Error: {str(err)}
            Query: {formatted_query}
            Parameters: {formatted_params}
            """
            logger.error(error_msg)
            self.log_failed_query(formatted_query, formatted_params)
            raise DatabaseConnectionError(error_msg)

        finally:
            elapsed_time = time.time() - start_time
            if elapsed_time > self.config.get('long_query_threshold', 60):
                logger.warning(f"Query took too long ({elapsed_time:.2f} seconds)")

    def execute_batch_query(self, query: str, values: List[tuple], batch_size: int = 1000):
        """
        Execute a batch of PostgreSQL queries with improved performance.

        Args:
            query: SQL query to execute
            values: List of parameter tuples
            batch_size: Size of each batch
        """
        with self.get_connection() as connection:
            try:
                with connection.cursor() as cursor:
                    # Use execute_values for better performance
                    execute_values(cursor, query, values, page_size=batch_size)
                    connection.commit()
                    logger.info(f"Processed {len(values)} records in batches of {batch_size}")

            except (OperationalError, DatabaseError) as err:
                error_msg = f"""
                Batch Error: {str(err)}
                Query: {query}
                Batch size: {batch_size}
                """
                logger.error(error_msg)
                connection.rollback()
                raise DatabaseConnectionError(error_msg)

    def execute_transaction(self, queries: List[tuple]):
        """
        Execute multiple queries as a transaction.

        Args:
            queries: List of (query, params) tuples
        """
        with self.get_connection() as connection:
            try:
                with connection.cursor() as cursor:
                    for query, params in queries:
                        formatted_query, formatted_params = self._format_query_and_params(query, params)
                        cursor.execute(formatted_query, formatted_params or ())

                connection.commit()
                logger.info("Transaction executed successfully.")

            except (OperationalError, DatabaseError) as err:
                error_msg = f"Transaction failed: {str(err)}"
                logger.error(error_msg)
                connection.rollback()
                raise DatabaseConnectionError(error_msg)

    def execute_query_with_savepoint(self, query: str, params: Optional[Dict[str, Any]] = None):
        """
        Execute a query with a savepoint for better transaction control.

        Args:
            query: SQL query to execute
            params: Query parameters
        """
        with self.get_connection() as connection:
            try:
                with connection.cursor() as cursor:
                    cursor.execute("SAVEPOINT query_savepoint")

                    formatted_query, formatted_params = self._format_query_and_params(query, params)
                    cursor.execute(formatted_query, formatted_params or ())

                    connection.commit()
                    logger.info("Query executed successfully with savepoint.")

            except (OperationalError, DatabaseError) as err:
                error_msg = f"Query failed with savepoint: {str(err)}"
                logger.error(error_msg)
                cursor.execute("ROLLBACK TO SAVEPOINT query_savepoint")
                connection.rollback()
                raise DatabaseConnectionError(error_msg)

    def log_failed_query(self, query: str, params: Optional[Dict[str, Any]] = None):
        """Log failed queries for retry attempts."""
        try:
            failed_query_data = {
                'query': query,
                'params': json.dumps(params or {}),
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                'retry_count': 0
            }
            failed_query_queue.put(failed_query_data)
            logger.info("Failed query logged for retry.")
        except Exception as err:
            logger.error(f"Failed to log failed query: {err}")
            with open('failed_queries.log', 'a') as f:
                f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {query} | {params}\n")

    def disconnect(self):
        """Close all database connections."""
        if self.connection_pool:
            if hasattr(self._local, 'connection') and self._local.connection:
                try:
                    self.connection_pool.putconn(self._local.connection)
                except:
                    pass
                finally:
                    self._local.connection = None

            self.connection_pool.closeall()
            self.connection_pool = None
            logger.info("PostgreSQL Database disconnected successfully.")
