from typing import Any, Dict, List, Optional, Tuple
import logging
from datetime import date, datetime

logger = logging.getLogger(__name__)

class SQLServerGenericCRUD:
    """Generic CRUD operations for any table in SQL Server."""

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

    def _infer_column_types(self, values: List[Tuple[Any]], columns: List[str]) -> Dict[str, str]:
        """
        Infer column data types based on the values provided, considering multiple rows for better accuracy.

        Args:
            values (list of tuples): List of tuples representing the values to insert.
            columns (list): List of column names.

        Returns:
            dict: Dictionary of column names and their inferred data types.
        """
        types = {}
        if values:
            for column in columns:
                # Loop through all rows to infer the data type across multiple values
                for row in values:
                    value = row[columns.index(column)]
                    if isinstance(value, int):
                        types[column] = 'INT'
                    elif isinstance(value, float):
                        types[column] = 'FLOAT'
                    elif isinstance(value, str):
                        types[column] = 'VARCHAR(MAX)'
                    elif isinstance(value, date):
                        types[column] = 'DATE'
                    elif isinstance(value, datetime):
                        types[column] = 'DATETIME'
                    else:
                        types[column] = 'VARCHAR(MAX)'
                    break  # Stop checking once we infer the type
        return types

    def create_table_if_not_exists(self, table: str, columns: List[str], values: List[Tuple[Any]]) -> bool:
        """
        Create a table with the specified columns if it does not already exist.

        Args:
            table (str): The name of the table to create.
            columns (list): List of column names.
            values (list of tuples): List of tuples representing the values to insert.

        Returns:
            bool: True if the table was created, False if it already existed.
        """
        existing_columns = self._get_table_columns(table)
        if existing_columns:
            logger.info(f"Table '{table}' already exists.")
            return False

        column_types = self._infer_column_types(values, columns)
        columns_def = ", ".join([f"{col} {dtype}" for col, dtype in column_types.items()])
        create_query = f"CREATE TABLE {table} ({columns_def})"

        try:
            self.db_client.execute_query(create_query)
            logger.info(f"Table '{table}' created successfully.")
            return True
        except Exception as e:
            logger.error(f"Failed to create table '{table}'. Error: {e}")
            raise

    def create(self, table: str, values: List[Tuple[Any]], columns: List[str] = None) -> bool:
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

        self.create_table_if_not_exists(table, columns, values)

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

    def execute_raw_query(self, query: str, params: Optional[Dict[str, Any]] = None, batch: bool = False) -> Optional[List[Dict[str, Any]]]:
        """
        Execute a raw SQL query (SELECT or non-SELECT).

        Args:
            query (str): The SQL query to execute.
            params (dict or list of dict, optional): A dictionary of parameters for single queries or a list of dictionaries for batch queries.
            batch (bool, optional): If True, execute the query as a batch operation. Default is False.

        Returns:
            Optional[list]: If the query is a SELECT query, returns a list of dictionaries representing rows. Otherwise, returns None.
        """
        connection = self.db_client.get_new_connection()  # Ensure a new connection for this task
        cursor = connection.cursor()
        try:
            # Check if the query is a SELECT query
            is_select_query = query.strip().lower().startswith('select')

            # Handle SELECT queries
            if is_select_query:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)

                # Fetch all rows and return as a list of dictionaries
                columns = [column[0] for column in cursor.description]
                rows = cursor.fetchall()
                result = [dict(zip(columns, row)) for row in rows]
                logger.info(f"Raw SELECT query executed successfully, {len(result)} rows fetched.")
                return result if rows else []

            # Handle non-SELECT queries (like INSERT, UPDATE, DELETE)
            else:
                if batch and isinstance(params, list):
                    # Batch execution for non-SELECT queries
                    cursor.fast_executemany = True  # Enable fast executemany
                    cursor.executemany(query, params)
                else:
                    # Single execution
                    cursor.execute(query, params or ())

                connection.commit()
                logger.info(f"Raw non-SELECT query executed successfully, affected rows: {cursor.rowcount}.")
                return None

        except Exception as e:
            logger.error(f"Failed to execute raw query. Error: {e}")
            connection.rollback()  # Rollback in case of error
            raise
        finally:
            # Ensure cursor and connection are closed properly
            cursor.close()
            connection.close()
