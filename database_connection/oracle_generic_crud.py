import re
from typing import Any, Dict, List, Optional, Tuple
import logging
from datetime import date, datetime
from .utils import retry

logger = logging.getLogger(__name__)

class OracleGenericCRUD:
    """Generic CRUD operations for any Oracle table."""

    def __init__(self, db_client):
        """
        Initialize the OracleGenericCRUD class.

        Args:
            db_client: An instance of a database client (e.g., OracleClient).
        """
        self.db_client = db_client

    def _get_table_columns(self, table: str, show_id: bool = False) -> List[str]:
        """
        Get the column names of a table, optionally including the 'id' column.

        Args:
            table (str): The table name.
            show_id (bool): If True, include the 'id' column. Default is False.

        Returns:
            list: List of column names.
        """
        # Use string concatenation for table name instead of bind variable
        # since Oracle doesn't support bind variables for object names
        query = f"""
            SELECT COLUMN_NAME
            FROM ALL_TAB_COLUMNS
            WHERE TABLE_NAME = '{table.upper()}'
        """

        if not show_id:
            query += " AND COLUMN_NAME != 'ID'"

        query += " ORDER BY COLUMN_ID"

        try:
            # Execute the query without params since we're using string concatenation
            result = self.db_client.execute_query(query, None, fetch_as_dict=True)

            # Extract the column names from the result
            columns = [row['COLUMN_NAME'] for row in result]

            logger.debug(f"Retrieved columns for table {table}: {columns}")
            return columns
        except Exception as e:
            # Log detailed error messages including query and table name
            logger.error(f"Error getting columns for table {table}. Query: {query}")
            logger.error(f"Failed to get table columns. Error: {str(e)}")
            raise

    def _format_dates(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format date fields in a record to a readable string format.

        Args:
            record (dict): The record with potential date fields.

        Returns:
            dict: The record with formatted date fields.
        """
        for key, value in record.items():
            if isinstance(value, (date, datetime)):
                record[key] = value.strftime('%Y-%m-%d %H:%M:%S') if isinstance(value, datetime) else value.strftime('%Y-%m-%d')
        return record

    def _infer_column_types(self, values: List[Tuple[Any]], columns: List[str], primary_key: str = None) -> Dict[str, str]:
        """
        Infer column data types based on provided values.

        Args:
            values (list of tuples): List of tuples representing rows of data.
            columns (list of str): List of column names.
            primary_key (str, optional): Primary key column name to include.

        Returns:
            dict: A dictionary mapping column names to their inferred Oracle data types.
        """
        type_mapping = {
            int: "NUMBER",
            float: "FLOAT",
            str: "VARCHAR2(255)",
            date: "DATE",
            datetime: "TIMESTAMP",
        }

        inferred_types = {}
        for idx, column in enumerate(columns):
            sample_value = next((row[idx] for row in values if row[idx] is not None), None)
            if sample_value is None:
                inferred_types[column] = "VARCHAR2(255)"  # Default type if no non-None value found
            else:
                inferred_types[column] = type_mapping.get(type(sample_value), "VARCHAR2(255)")

            # If the column is the primary key, mark it accordingly
            if column == primary_key:
                inferred_types[column] += " PRIMARY KEY"

        return inferred_types

    def _validate_table_name(self, table_name: str) -> bool:
        """Validate that a table name contains only allowed characters."""
        # Only allow alphanumeric characters and underscores
        pattern = re.compile(r'^[A-Za-z0-9_]+$')
        return bool(pattern.match(table_name))

    def create_table_if_not_exists(self, table: str, columns: List[str], values: List[Tuple[Any]], primary_key: str = None) -> None:
        """
        Create a table with the specified columns if it does not already exist.

        Args:
            table (str): The name of the table to create.
            columns (list): List of column names.
            values (list of tuples): List of tuples representing the values to insert.
            primary_key (str, optional): The column to set as primary key.

        Returns:
            None
        """
        try:

            # Validate table name before using it in query
            if not self._validate_table_name(table):
                raise ValueError(f"Invalid table name: {table}")

            # First check if the table exists using a different query
            check_query = f"""
                SELECT COUNT(*) AS TABLE_COUNT
                FROM ALL_TABLES
                WHERE TABLE_NAME = '{table.upper()}'
            """
            result = self.db_client.execute_query(check_query, None, fetch_as_dict=True)
            table_exists = result[0]['TABLE_COUNT'] > 0
            if table_exists:
                logger.info(f"Table '{table}' already exists.")
                return

            # Ensure primary key is in the list of columns
            if primary_key and primary_key not in columns:
                logger.info(f"Primary key '{primary_key}' not found in columns. Adding it.")
                columns.append(primary_key)

            # Infer column data types
            column_types = self._infer_column_types(values, columns, primary_key)

            # Generate the CREATE TABLE query
            columns_def = ", ".join([f"{col} {dtype}" for col, dtype in column_types.items()])
            create_query = f"CREATE TABLE {table} ({columns_def})"

            # Execute the query
            self.db_client.execute_query(create_query)
            logger.info(f"Table '{table}' created successfully.")
        except Exception as e:
            logger.error(f"Failed to create table '{table}'. Error: {str(e)}")
            raise

    @retry(max_retries=5, delay=5, backoff=2, exceptions=(Exception,), logger=logger)
    def create(self, table: str, values: List[Tuple[Any]], columns: List[str] = None, primary_key: str = None) -> bool:
        """
        Create new records in the specified table.

        Args:
            table (str): The table name.
            values (list of tuples): List of tuples of values to insert.
            columns (list, optional): List of column names. If None, columns will be inferred from the table schema.
            primary_key (str, optional): Primary key column name.

        Returns:
            bool: True if records were inserted successfully, False otherwise.
        """
        if columns is None:
            columns = self._get_table_columns(table)

        # Ensure values is a list of tuples
        if not isinstance(values, list):
            values = [values]
        elif not all(isinstance(v, tuple) for v in values):
            raise ValueError("Values must be a tuple or a list of tuples.")

        # Create the table if it doesn't exist
        self.create_table_if_not_exists(table, columns, values, primary_key)

        for value_tuple in values:
            if len(value_tuple) != len(columns):
                raise ValueError(f"Number of values {len(value_tuple)} does not match number of columns {len(columns)}")

        columns_str = ", ".join(columns)
        placeholders = ", ".join([f":{i+1}" for i in range(len(columns))])
        query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"

        try:
            self.db_client.execute_batch_query(query, values)
            logger.info("Records inserted.")
            return True
        except Exception as e:
            logger.error(f"Failed to insert records. Error: {e}")
            return False

    @retry(max_retries=5, delay=5, backoff=2, exceptions=(Exception,), logger=logger)
    def read(self, table: str, columns: List[str] = None, where: str = "", params: Tuple[Any] = None, show_id: bool = False, batch_size: int = None) -> List[Dict[str, Any]]:
        """
        Read records from the specified table with optional batch support.

        Args:
            table (str): The table name.
            columns (list, optional): List of column names to retrieve. If None, all columns will be retrieved.
            where (str, optional): WHERE clause for filtering records.
            params (tuple, optional): Tuple of parameters for the WHERE clause.
            show_id (bool, optional): If True, include the 'id' column. Default is False.
            batch_size (int, optional): If provided, limits the number of records returned per batch.

        Returns:
            list: List of records as dictionaries.
        """
        if columns is None:
            columns = self._get_table_columns(table, show_id=show_id)

        columns_str = ", ".join(columns) if columns else "*"
        query = f"SELECT {columns_str} FROM {table}"
        if where:
            query += f" WHERE {where}"
        if batch_size:
            query += f" FETCH NEXT {batch_size} ROWS ONLY"

        try:
            result = self.db_client.execute_query(query, params, fetch_as_dict=True)
            records = [self._format_dates(row) for row in result]
            logger.info(f"Records read successfully, {len(records)} rows found.")
            return records
        except Exception as e:
            logger.error(f"Failed to read records. Error: {e}")
            raise

    @retry(max_retries=5, delay=5, backoff=2, exceptions=(Exception,), logger=logger)
    def update(self, table: str, updates: Dict[str, Any], where: str, params: Tuple[Any]) -> bool:
        """
        Update records in the specified table.

        Args:
            table (str): The table name.
            updates (dict): Dictionary of columns and their new values.
            where (str): WHERE clause for identifying records to update.
            params (tuple): Tuple of parameters for the WHERE clause.

        Returns:
            bool: True if records were updated successfully, False otherwise.
        """
        set_clause = ", ".join([f"{col} = :{i+1}" for i, col in enumerate(updates.keys(), 1)])
        query = f"UPDATE {table} SET {set_clause} WHERE {where}"
        values = tuple(updates.values()) + params
        try:
            self.db_client.execute_query(query, values)
            logger.info("Records updated successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to update records. Error: {e}")
            return False

    @retry(max_retries=5, delay=5, backoff=2, exceptions=(Exception,), logger=logger)
    def delete(self, table: str, where: str = "", params: Tuple[Any] = None) -> bool:
        """
        Delete records from the specified table.

        Args:
            table (str): The table name.
            where (str, optional): WHERE clause for identifying records to delete. If empty, all records will be deleted.
            params (tuple, optional): Tuple of parameters for the WHERE clause.

        Returns:
            bool: True if records were deleted successfully, False otherwise.
        """
        query = f"DELETE FROM {table}"
        if where:
            query += f" WHERE {where}"

        try:
            self.db_client.execute_query(query, params)
            logger.info("Records deleted successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to delete records. Error: {e}")
            return False

    def begin_transaction(self):
        """Begin a transaction."""
        try:
            self.db_client.begin_transaction()
        except Exception as e:
            logger.error(f"Failed to begin transaction. Error: {e}")
            raise

    def commit_transaction(self):
        """Commit the current transaction."""
        try:
            self.db_client.commit_transaction()
            logger.info("Transaction committed successfully.")
        except Exception as e:
            logger.error(f"Failed to commit transaction. Error: {e}")
            raise

    def rollback_transaction(self):
        """Rollback the current transaction."""
        try:
            self.db_client.rollback_transaction()
            logger.info("Transaction rolled back successfully.")
        except Exception as e:
            logger.error(f"Failed to rollback transaction. Error: {e}")
            raise
