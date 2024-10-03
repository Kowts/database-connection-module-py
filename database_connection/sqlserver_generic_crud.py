from collections import OrderedDict
from typing import Any, Dict, List, Optional, Tuple
import logging
from datetime import date, datetime
from .utils import retry

logger = logging.getLogger(__name__)

class SQLServerGenericCRUD:
    """Enhanced Generic CRUD operations for any table in SQL Server."""

    def __init__(self, db_client):
        """
        Initialize the SQLServerGenericCRUD class.

        Args:
            db_client: An instance of a database client (e.g., SQLServerClient).
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
        query = """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = ?"""
        if not show_id:
            query += " AND COLUMN_NAME != 'ID'"
        query += " ORDER BY ORDINAL_POSITION"

        try:
            result = self.db_client.execute_query(query, (table,), fetch_as_dict=True)
            columns = [row['COLUMN_NAME'] for row in result]
            return columns
        except Exception as e:
            logger.error(f"Failed to get table columns. Error: {e}")
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
        Infer column data types based on the values provided, considering multiple rows for better accuracy.
        Optionally mark a specified column as the PRIMARY KEY, retaining its original inferred type.

        Args:
            values (list of tuples): List of tuples representing the values to insert.
            columns (list): List of column names.
            primary_key (str, optional): Column name to be set as the PRIMARY KEY, retaining its original inferred type.

        Returns:
            dict: Dictionary of column names and their inferred data types, with the primary key marked if specified.
        """
        types = {}

        if values:
            for column in columns:
                inferred_type = None
                max_int_digits = 1  # Default to 1 digit for INT columns

                for row in values:
                    value = row[columns.index(column)]

                    if isinstance(value, int):
                        max_int_digits = max(max_int_digits, len(str(abs(value))))
                        if inferred_type not in ['FLOAT', 'VARCHAR(MAX)']:
                            inferred_type = 'INT'
                    elif isinstance(value, float):
                        if inferred_type != 'VARCHAR(MAX)':
                            inferred_type = 'FLOAT'
                    elif isinstance(value, str):
                        inferred_type = 'VARCHAR(MAX)'
                    elif isinstance(value, date):
                        inferred_type = 'DATE'
                    elif isinstance(value, datetime):
                        inferred_type = 'DATETIME'
                    else:
                        inferred_type = 'VARCHAR(MAX)'

                if inferred_type == 'INT' and max_int_digits > 11:
                    inferred_type = 'BIGINT'

                if column == primary_key:
                    inferred_type = f"{inferred_type} PRIMARY KEY"

                types[column] = inferred_type or 'VARCHAR(MAX)'

        return types

    def create_table_if_not_exists(self, table: str, columns: List[str], values: List[Tuple[Any]], primary_key: str) -> bool:
        """
        Create a table with the specified columns if it does not already exist.

        Args:
            table (str): The name of the table to create.
            columns (list): List of column names.
            values (list of tuples): List of tuples representing the values to insert.
            primary_key (str): The column to set as the primary key.

        Returns:
            bool: True if the table was created, False if it already existed.
        """
        # Get the existing columns in the table
        existing_columns = self._get_table_columns(table)
        if existing_columns:
            logger.info(f"Table '{table}' already exists.")
            return False

        # Ensure the primary key column is in the columns list
        if primary_key not in columns:
            logger.info(f"Primary key column '{primary_key}' not found in columns. Adding it.")
            columns.append(primary_key)

        # Infer column types based on the provided values
        column_types = OrderedDict()
        inferred_types = self._infer_column_types(values, columns, primary_key)

        # Update the ordered dictionary with the inferred types
        column_types.update(inferred_types)

        # Construct the column definitions for the CREATE TABLE query
        columns_def = ", ".join([f"{col} {dtype}" for col, dtype in column_types.items()])
        create_query = f"CREATE TABLE {table} ({columns_def})"

        try:
            self.db_client.execute_query(create_query)
            logger.info(f"Table '{table}' created successfully.")
            return True
        except Exception as e:
            logger.error(f"Failed to create table '{table}'. Error: {e}")
            raise

    @retry(max_retries=5, delay=5, backoff=2, exceptions=(Exception,), logger=logger)
    def create(self, table: str, values: List[Tuple[Any]], columns: List[str] = None, primary_key: str = None) -> bool:
        """
        Create new records in the specified table.

        Args:
            table (str): The table name.
            values (list of tuples): List of tuples of values to insert.
            columns (list, optional): List of column names. If None, columns will be inferred from the table schema.

        Returns:
            bool: True if records were inserted successfully, False otherwise.
        """
        if columns is None:
            columns = self._get_table_columns(table)

        if not isinstance(values, list):
            values = [values]
        elif not all(isinstance(v, tuple) for v in values):
            raise ValueError("Values must be a tuple or a list of tuples.")

        self.create_table_if_not_exists(table, columns, values, primary_key=primary_key)

        for value_tuple in values:
            if len(value_tuple) != len(columns):
                raise ValueError(f"Number of values {len(value_tuple)} does not match number of columns {len(columns)}")

        columns_str = ", ".join(columns)
        placeholders = ", ".join(["?" for _ in columns])
        query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"

        try:
            self.db_client.execute_batch_query(query, values)
            logger.info("Records inserted")
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
            query += f" ORDER BY {columns[0]} OFFSET 0 ROWS FETCH NEXT {batch_size} ROWS ONLY"

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
        set_clause = ", ".join([f"{col} = ?" for col in updates.keys()])
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
    def delete(self, table: str, where: str = "", params: Tuple[Any] = None, batch_size: int = None) -> bool:
        """
        Delete records from the specified table with optional batch processing.

        Args:
            table (str): The table name.
            where (str, optional): WHERE clause for identifying records to delete. If empty, all records will be deleted.
            params (tuple, optional): Tuple of parameters for the WHERE clause.
            batch_size (int, optional): If provided, deletes records in batches.

        Returns:
            bool: True if records were deleted successfully, False otherwise.
        """
        query = f"DELETE FROM {table}"
        if where:
            query += f" WHERE {where}"
        if batch_size:
            query += f" ORDER BY (SELECT NULL) OFFSET 0 ROWS FETCH NEXT {batch_size} ROWS ONLY"

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
