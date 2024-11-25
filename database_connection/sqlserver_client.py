import json
import pyodbc
import logging
import queue
from utils import retry
from .base_database import BaseDatabase, DatabaseConnectionError
import threading
import time
from typing import Any, Dict, List, Optional, Tuple, Union
from contextlib import contextmanager

# Create a queue to hold failed queries
failed_query_queue = queue.Queue()
MAX_RETRIES = 3

logger = logging.getLogger(__name__)

class SQLServerClient(BaseDatabase):
    """SQL Server database connection and operations with enhanced functionality."""

    def __init__(self, config):
        """
        Initialize the SQLServerClient class.

        Args:
            config (dict): Configuration parameters for the SQL Server connection.
        """
        super().__init__(config)
        self._local = threading.local()
        self._connection_lock = threading.Lock()
        self._connection_string = None

    def connect(self):
        """
        Establish the SQL Server database connection.
        Sets up connection pooling and configuration.
        """
        try:
            self._connection_string = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={self.config['server']},{self.config['port']};"
                f"DATABASE={self.config['database']};"
                f"UID={self.config['login']};"
                f"PWD={self.config['password']};"
                f"Encrypt=no;"
                f"TrustServerCertificate=yes;"
                f"MultipleActiveResultSets=true;"
                f"Connection Pooling=true;"
                f"Min Pool Size={self.config.get('min_pool_size', 2)};"
                f"Max Pool Size={self.config.get('max_pool_size', 10)};"
                f"Connection Timeout={self.config.get('connection_timeout', 30)};"
                f"Command Timeout={self.config.get('command_timeout', 30)};"
                f"{self.config.get('additional_params', '')}"
            )

            # Test connection
            with self._get_new_connection() as test_conn:
                logger.info("SQL Server Database connection pool configured successfully.")

        except pyodbc.Error as err:
            logger.error(f"Error connecting to SQL Server Database: {err}")
            raise DatabaseConnectionError(err)

    @contextmanager
    def _get_new_connection(self):
        """Internal method to get a new connection."""
        connection = None
        try:
            connection = pyodbc.connect(self._connection_string, autocommit=False)
            yield connection
        finally:
            if connection:
                try:
                    connection.close()
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
                    self._local.connection = pyodbc.connect(self._connection_string, autocommit=False)

            yield self._local.connection

        except Exception as e:
            if hasattr(self._local, 'connection') and self._local.connection:
                try:
                    self._local.connection.rollback()
                    self._local.connection.close()
                except:
                    pass
                finally:
                    self._local.connection = None
            raise e

    def _format_query_and_params(self, query: str,
                                params: Optional[Union[Dict[str, Any], Tuple[Any], List[Any]]] = None) -> Tuple[str, Any]:
        """
        Format query and parameters, handling table date replacement.

        Args:
            query: SQL query with potential {tableDate} placeholder
            params: Query parameters

        Returns:
            Tuple of (formatted query, formatted parameters)
        """
        try:
            if params is None:
                return query, None

            if isinstance(params, dict):
                if 'tableDate' in params:
                    formatted_query = query.replace("{tableDate}", str(params['tableDate']))
                    remaining_params = {k: v for k, v in params.items() if k != 'tableDate'}
                    formatted_params = tuple(remaining_params.values()) if remaining_params else None
                    return formatted_query, formatted_params
                return query, tuple(params.values())

            if isinstance(params, (tuple, list)):
                if '{tableDate}' in query and len(params) > 0:
                    formatted_query = query.replace("{tableDate}", str(params[0]))
                    formatted_params = params[1:] if len(params) > 1 else None
                    return formatted_query, formatted_params
                return query, tuple(params)

            return query, None

        except Exception as e:
            logger.error(f"Error formatting query: {e}")
            raise ValueError(f"Query formatting failed: {e}")

    @retry(max_retries=5, delay=10, backoff=2, exceptions=(pyodbc.Error,), logger=logger)
    def execute_query(self, query: str,
                     params: Optional[Union[Dict[str, Any], Tuple[Any], List[Any]]] = None,
                     fetch_as_dict: bool = False,
                     timeout: Optional[int] = None) -> Any:
        """
        Execute a SQL Server query with enhanced error handling.

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
            # Format query and parameters
            formatted_query, formatted_params = self._format_query_and_params(query, params)

            # Log query details
            logger.debug(f"Executing query: {formatted_query}")
            logger.debug(f"Parameters: {formatted_params}")

            with self.get_connection() as connection:
                cursor = connection.cursor()

                try:
                    if timeout:
                        cursor.timeout = timeout

                    # Execute the query
                    cursor.execute(formatted_query, formatted_params or ())

                    # Handle SELECT queries
                    if formatted_query.strip().lower().startswith("select"):
                        if fetch_as_dict:
                            columns = [column[0] for column in cursor.description]
                            rows = cursor.fetchall()
                            result = [dict(zip(columns, row)) for row in rows]
                        else:
                            result = cursor.fetchall()
                    else:
                        result = cursor.rowcount
                        connection.commit()

                    return result

                except pyodbc.Error as err:
                    error_msg = f"""
                    SQL Error: {str(err)}
                    Query: {formatted_query}
                    Parameters: {formatted_params}
                    """
                    logger.error(error_msg)
                    connection.rollback()
                    self.log_failed_query(formatted_query, formatted_params)
                    raise DatabaseConnectionError(error_msg)

                finally:
                    cursor.close()
                    elapsed_time = time.time() - start_time
                    if elapsed_time > self.config.get('long_query_threshold', 60):
                        logger.warning(f"Query took too long ({elapsed_time:.2f} seconds)")

        except Exception as err:
            error_msg = f"Unexpected error: {str(err)}"
            logger.error(error_msg)
            raise DatabaseConnectionError(error_msg)

    def execute_batch_query(self, query: str, values: List[tuple], batch_size: int = 1000):
        """
        Execute a batch of SQL Server queries without progress logging.

        Args:
            query: SQL query to execute
            values: List of parameter tuples
            batch_size: Size of each batch
        """
        with self.get_connection() as connection:
            try:
                cursor = connection.cursor()
                cursor.fast_executemany = True

                for i in range(0, len(values), batch_size):
                    batch = values[i:i + batch_size]
                    cursor.executemany(query, batch)
                    connection.commit()

            except pyodbc.Error as err:
                error_msg = f"""
                Batch Error: {str(err)}
                Query: {query}
                Batch size: {batch_size}
                """
                logger.error(error_msg)
                connection.rollback()
                raise DatabaseConnectionError(error_msg)

            finally:
                cursor.close()

    def execute_transaction(self, queries: List[tuple]):
        """
        Execute multiple queries as a transaction.

        Args:
            queries: List of (query, params) tuples
        """
        with self.get_connection() as connection:
            try:
                cursor = connection.cursor()

                for query, params in queries:
                    formatted_query, formatted_params = self._format_query_and_params(query, params)
                    cursor.execute(formatted_query, formatted_params or ())

                connection.commit()
                logger.info("Transaction executed successfully.")

            except pyodbc.Error as err:
                error_msg = f"Transaction failed: {str(err)}"
                logger.error(error_msg)
                connection.rollback()
                raise DatabaseConnectionError(error_msg)

            finally:
                cursor.close()

    def execute_query_with_savepoint(self, query: str, params: Optional[Dict[str, Any]] = None):
        """
        Execute a query with a savepoint for better transaction control.

        Args:
            query: SQL query to execute
            params: Query parameters
        """
        with self.get_connection() as connection:
            try:
                cursor = connection.cursor()
                cursor.execute("SAVE TRANSACTION query_savepoint")

                formatted_query, formatted_params = self._format_query_and_params(query, params)
                cursor.execute(formatted_query, formatted_params or ())

                connection.commit()
                logger.info("Query executed successfully with savepoint.")

            except pyodbc.Error as err:
                error_msg = f"Query failed with savepoint: {str(err)}"
                logger.error(error_msg)
                cursor.execute("ROLLBACK TRANSACTION query_savepoint")
                connection.rollback()
                raise DatabaseConnectionError(error_msg)

            finally:
                cursor.close()

    def log_failed_query(self, query: str, params: Optional[Dict[str, Any]] = None):
        """
        Log failed queries for retry attempts.

        Args:
            query: Failed query
            params: Query parameters
        """
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
        if hasattr(self._local, 'connection') and self._local.connection:
            try:
                self._local.connection.close()
            except:
                pass
            finally:
                self._local.connection = None
        logger.info("SQL Server Database disconnected successfully.")
