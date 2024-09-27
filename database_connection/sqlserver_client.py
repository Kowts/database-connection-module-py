import pyodbc
import logging
from .base_database import BaseDatabase, DatabaseConnectionError
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

class SQLServerClient(BaseDatabase):
    """SQL Server database connection and operations."""

    def connect(self):
        """Establish the SQL Server database connection."""
        try:
            connection_string = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={self.config['server']},{self.config['port']};"
                f"DATABASE={self.config['database']};"
                f"UID={self.config['login']};"
                f"PWD={self.config['password']};"
                f"Encrypt=no;TrustServerCertificate=yes;"
                f"MultipleActiveResultSets=true;"
                f"Connection Pooling=true;"
                f"Max Pool Size={self.config.get('max_pool_size', 100)};"
                f"{self.config.get('additional_params', '')}"
            )
            self.connection = pyodbc.connect(connection_string)
            logger.info("SQL Server Database connected successfully.")
        except pyodbc.Error as err:
            logger.error(f"Error connecting to SQL Server Database: {err}")
            raise DatabaseConnectionError(err)

    def get_new_connection(self) -> pyodbc.Connection:
        """Get a new connection for parallel tasks."""
        try:
            connection_string = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={self.config['server']},{self.config['port']};"
                f"DATABASE={self.config['database']};"
                f"UID={self.config['login']};"
                f"PWD={self.config['password']};"
                f"Encrypt=no;TrustServerCertificate=yes;"
                f"MultipleActiveResultSets=true;"
                f"Connection Pooling=true;"
                f"Max Pool Size={self.config.get('max_pool_size', 100)};"
                f"{self.config.get('additional_params', '')}"
            )
            connection = pyodbc.connect(connection_string)
            logger.info("New SQL Server Database connection created for parallel task.")
            return connection
        except pyodbc.Error as err:
            logger.error(f"Error creating new SQL Server Database connection: {err}")
            raise DatabaseConnectionError(err)

    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None, fetch_as_dict: bool = False) -> Optional[List[Dict[str, Any]]]:
        """
        Execute a SQL Server database query.

        Args:
            query (str): The SQL query to execute.
            params (dict, optional): Dictionary of parameters to bind to the query. Default is None.
            fetch_as_dict (bool, optional): If True, fetch results as a list of dictionaries. Default is False.

        Returns:
            Optional[list]: If the query is a SELECT query and fetch_as_dict is True, returns a list of dictionaries representing rows. Otherwise, returns the result.
        """
        start_time = time.time()
        cursor = None
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params or ())

            # Fetch the results if it's a SELECT query
            if query.strip().lower().startswith("select"):
                if fetch_as_dict:
                    columns = [column[0] for column in cursor.description]
                    rows = cursor.fetchall()
                    result = [dict(zip(columns, row)) for row in rows]
                    return result
                else:
                    return cursor.fetchall()

            self.connection.commit()
            return cursor.rowcount

        except pyodbc.Error as err:
            logger.error(f"Failed to execute query. Error: {err}")
            self.connection.rollback()  # Ensure rollback on failure
            raise

        finally:
            if cursor:
                cursor.close()  # Ensure cursor is closed
            elapsed_time = time.time() - start_time
            if elapsed_time > self.config.get('long_query_threshold', 60):
                logger.warning(f"Query took too long ({elapsed_time} seconds): {query}")

    def execute_batch_query(self, query: str, params_list: List[Dict[str, Any]]) -> None:
        """
        Execute a batch of SQL Server queries.

        Args:
            query (str): The SQL query to execute.
            params_list (list of dict): List of dictionaries with parameters for the batch queries.

        Returns:
            None
        """
        start_time = time.time()
        cursor = None
        try:
            cursor = self.connection.cursor()
            cursor.fast_executemany = True
            cursor.executemany(query, params_list)
            self.connection.commit()
            logger.info("Batch query executed successfully.")
        except pyodbc.Error as err:
            logger.error(f"Failed to execute batch query. Error: {err}")
            self.connection.rollback()  # Ensure rollback on failure
            raise

        finally:
            if cursor:
                cursor.close()  # Ensure cursor is closed
            elapsed_time = time.time() - start_time
            if elapsed_time > self.config.get('long_query_threshold', 60):
                logger.warning(f"Batch query took too long ({elapsed_time} seconds): {query}")

    def begin_transaction(self):
        """Begin a database transaction."""
        try:
            self.connection.autocommit = False
            logger.info("Transaction started.")
        except pyodbc.Error as err:
            logger.error(f"Failed to start transaction. Error: {err}")
            raise

    def commit_transaction(self):
        """Commit the current transaction."""
        try:
            self.connection.commit()
            self.connection.autocommit = True
            logger.info("Transaction committed.")
        except pyodbc.Error as err:
            logger.error(f"Failed to commit transaction. Error: {err}")
            raise

    def rollback_transaction(self):
        """Rollback the current transaction."""
        try:
            self.connection.rollback()
            self.connection.autocommit = True
            logger.info("Transaction rolled back.")
        except pyodbc.Error as err:
            logger.error(f"Failed to rollback transaction. Error: {err}")
            raise

    def disconnect(self):
        """Close the SQL Server database connection."""
        if self.connection:
            self.connection.close()
            logger.info("SQL Server Database disconnected successfully.")
