import json
import pyodbc
import logging
from .base_database import BaseDatabase, DatabaseConnectionError
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

class SQLServerClient(BaseDatabase):
    """SQL Server database connection and operations."""

    def connect(self):
        """
        Establish the SQL Server database connection.
        Uses configuration details for server, database, login, password, and other parameters.
        Sets a timeout to prevent long-running connections.
        """
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
                f"timeout=30;"  # 30-second timeout to prevent long-running queries
                f"{self.config.get('additional_params', '')}"
            )
            self.connection = pyodbc.connect(connection_string)
            logger.info("SQL Server Database connected successfully.")
        except pyodbc.Error as err:
            logger.error(f"Error connecting to SQL Server Database: {err}")
            raise DatabaseConnectionError(err)

    def get_new_connection(self) -> pyodbc.Connection:
        """
        Get a new connection for parallel tasks.
        This ensures that tasks can run simultaneously without conflicting over shared connections.
        """
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
                f"timeout=30;"  # 30-second timeout to prevent long-running queries
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
        Execute a SQL Server query with retry and timeout handling.

        Args:
            query (str): The SQL query to execute.
            params (dict, optional): Dictionary of parameters to bind to the query. Default is None.
            fetch_as_dict (bool, optional): If True, fetch results as a list of dictionaries. Default is False.

        Returns:
            Optional[list]: A list of dictionaries if fetch_as_dict is True and it's a SELECT query.
                            Otherwise, returns the result or row count.
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

            # Commit the transaction for non-SELECT queries
            self.connection.commit()
            return cursor.rowcount

        except pyodbc.Error as err:
            # Handle specific SQL errors like deadlock or timeouts
            if '1205' in str(err):
                logger.error(f"Deadlock detected, retrying: {err}")
            elif 'SQLSTATE_TIMEOUT' in str(err):
                logger.error(f"Query timeout occurred: {err}")
            else:
                logger.error(f"Failed to execute query. Error: {err}")

            # Log the failed query for retry
            self.log_failed_query(query, params)
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
        Execute a batch of SQL Server queries, optimized for handling large datasets by splitting the batch.

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
            # Split the batch into smaller chunks for large datasets to avoid failures
            batch_size = 1000
            for i in range(0, len(params_list), batch_size):
                cursor.executemany(query, params_list[i:i + batch_size])
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

    def log_failed_query(self, query: str, params: Optional[Dict[str, Any]] = None, retry_count: int = 0):
        """
        Log the failed query for future re-execution.

        In case of a failure, the query is stored in the 'failed_queries' table or a log file for retry.
        Args:
            query (str): The failed query.
            params (dict, optional): Dictionary of parameters for the query. Default is None.
            retry_count (int): The current retry count for the query.
        """
        try:
            failure_query = """
            INSERT INTO failed_queries (query, params, failed_at, retry_count)
            VALUES (?, ?, GETDATE(), ?)
            """
            cursor = self.connection.cursor()
            cursor.execute(failure_query, query, json.dumps(params or {}), retry_count)
            self.connection.commit()
            logger.info("Logged failed query for retry.")
        except pyodbc.Error as err:
            logger.error(f"Failed to log failed query: {err}")
            # Fallback to logging in a file if the database is not accessible
            with open('failed_queries.log', 'a') as f:
                f.write(f"{query} | {params} | Retry Count: {retry_count}\n")
            self.connection.rollback()

    def retry_failed_queries(self):
        """
        Retry all failed queries stored in the 'failed_queries' table.

        Queries that failed previously will be re-executed. If they succeed, they are removed from the table.
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT id, query, params, retry_count FROM failed_queries WHERE retry_count < 5")

            failed_queries = cursor.fetchall()
            for query in failed_queries:
                query_id, query_text, params, retry_count = query
                params = json.loads(params)

                try:
                    cursor.execute(query_text, params)
                    self.connection.commit()
                    # Delete from failed queries if successful
                    cursor.execute("DELETE FROM failed_queries WHERE id = ?", query_id)
                    logger.info(f"Successfully re-executed failed query ID {query_id}.")
                except pyodbc.Error as err:
                    logger.error(f"Failed to retry query ID {query_id}: {err}")
                    # Increment retry count
                    cursor.execute("UPDATE failed_queries SET retry_count = retry_count + 1 WHERE id = ?", query_id)
                    self.connection.commit()

        except pyodbc.Error as err:
            logger.error(f"Error retrying failed queries: {err}")
        finally:
            if cursor:
                cursor.close()

    def execute_query_with_savepoint(self, query: str, params: Optional[Dict[str, Any]] = None):
        """
        Execute a query with a savepoint for better transaction control.

        In case the query fails, it rolls back to the savepoint without affecting other parts of the transaction.
        Args:
            query (str): The SQL query to execute.
            params (dict, optional): Dictionary of parameters to bind to the query. Default is None.
        """
        try:
            self.connection.autocommit = False  # Disable autocommit for transaction control
            cursor = self.connection.cursor()

            cursor.execute("SAVE TRANSACTION my_savepoint")  # Create a savepoint
            cursor.execute(query, params or ())  # Execute the query
            self.connection.commit()  # Commit transaction if successful
        except pyodbc.Error as err:
            logger.error(f"Failed to execute query, rolling back to savepoint. Error: {err}")
            cursor.execute("ROLLBACK TRANSACTION my_savepoint")  # Rollback to savepoint on failure
            self.connection.rollback()  # Rollback the entire transaction if needed
            raise
        finally:
            self.connection.autocommit = True
            if cursor:
                cursor.close()

    def cleanup_failed_queries(self, days: int = 30):
        """
        Remove old failed queries that exceed the retry limit or are older than a specified number of days.

        Args:
            days (int): Number of days after which the failed queries will be removed. Default is 30.
        """
        try:
            cursor = self.connection.cursor()
            cleanup_query = """
            DELETE FROM failed_queries
            WHERE failed_at < DATEADD(DAY, -?, GETDATE()) OR retry_count >= 5
            """
            cursor.execute(cleanup_query, days)
            self.connection.commit()
            logger.info(f"Cleaned up failed queries older than {days} days or exceeding retry limit.")
        except pyodbc.Error as err:
            logger.error(f"Failed to clean up old failed queries: {err}")
            self.connection.rollback()
        finally:
            if cursor:
                cursor.close()

    def disconnect(self):
        """Close the SQL Server database connection."""
        if self.connection:
            self.connection.close()
            logger.info("SQL Server Database disconnected successfully.")

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
