from typing import Any, Dict, List, Optional, Tuple
import logging
from datetime import date, datetime

logger = logging.getLogger(__name__)

class MySQLGenericCRUD:
    """Generic CRUD operations for any table using MySQL."""

    def __init__(self, db_client):
        """
        Initialize the MySQLGenericCRUD class.

        Args:
            db_client: An instance of a database client (e.g., MySQLClient).
        """
        self.db_client = db_client

    def _get_table_columns(self, table: str, show_id: bool = False) -> List[str]:
        """
        Retrieve the column names of a specified table from the database.

        Args:
            table (str): The name of the table to retrieve columns from.
            show_id (bool, optional): If True, include the 'id' column in the result. Defaults to False.

        Returns:
            List[str]: A list of column names for the specified table.

        Raises:
            Exception: If the query fails or an error occurs during execution.
        """
        query = f"""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = %s
        AND TABLE_SCHEMA = DATABASE()
        """
        if not show_id:
            query += "AND COLUMN_NAME != 'id' "
        query += "ORDER BY ORDINAL_POSITION"

        try:
            # Execute the query and fetch the result as a dictionary
            result = self.db_client.execute_query(query, (table,), fetch_as_dict=True)
            columns = [row['COLUMN_NAME'] for row in result]
            return columns
        except Exception as e:
            logger.error(f"Failed to get table columns for '{table}'. Error: {e}")
            raise

    def _infer_column_types(self, values: List[Tuple[Any]], columns: List[str]) -> Dict[str, str]:
        """
        Infers the MySQL column types based on sample data provided for each column.

        Args:
            values (List[Tuple[Any]]): A list of tuples representing rows of data to infer types from.
            columns (List[str]): A list of column names corresponding to the data.

        Returns:
            Dict[str, str]: A dictionary mapping each column name to its inferred MySQL data type.
        """
        type_mapping = {
            int: "INT",
            float: "FLOAT",
            str: "VARCHAR(255)",
            date: "DATE",
            datetime: "DATETIME",
        }

        inferred_types = {}
        for idx, column in enumerate(columns):
            # Find the first non-None value in the current column to infer the type
            sample_value = next((row[idx] for row in values if row[idx] is not None), None)
            if sample_value is None:
                # Default to VARCHAR(255) if all values are None
                inferred_types[column] = "VARCHAR(255)"
            else:
                # Map Python type to MySQL type
                inferred_types[column] = type_mapping.get(type(sample_value), "VARCHAR(255)")

        return inferred_types

    def create_table_if_not_exists(self, table: str, columns: List[str], values: List[Tuple[Any]]) -> None:
        """
        Creates a table in the MySQL database if it does not already exist, using the provided column names and values.

        Args:
            table (str): The name of the table to create.
            columns (List[str]): A list of column names for the new table.
            values (List[Tuple[Any]]): Sample data to infer the column types.

        Raises:
            Exception: If the table creation fails for any reason.
        """
        try:
            # Check if the table already exists by retrieving its columns
            existing_columns = self._get_table_columns(table)
            if existing_columns:
                logger.info(f"Table '{table}' already exists.")
                return

            # Infer column data types from the provided sample data
            column_types = self._infer_column_types(values, columns)

            # Generate the CREATE TABLE SQL query
            columns_def = ", ".join([f"{col} {dtype}" for col, dtype in column_types.items()])
            create_query = f"CREATE TABLE {table} ({columns_def})"

            # Execute the CREATE TABLE query
            self.db_client.execute_query(create_query)
            logger.info(f"Table '{table}' created successfully.")
        except Exception as e:
            logger.error(f"Failed to create table '{table}'. Error: {e}")
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

    def create(self, table: str, values: List[Tuple[Any]], columns: List[str] = None) -> None:
        """
        Create new records in the specified table.

        Args:
            table (str): The table name.
            values (list of tuples): List of tuples of values to insert.
            columns (list, optional): List of column names. If None, columns will be inferred from the table schema.
        """
        # Check if the table already exists by retrieving its columns
        existing_columns = self._get_table_columns(table)
        if not existing_columns:
            logger.info(f"Table '{table}' does not exist. Creating the table...")
            # If no columns found, create the table
            if columns is None:
                raise ValueError("Columns must be provided when creating a new table.")
            self.create_table_if_not_exists(table, columns, values)
            existing_columns = self._get_table_columns(table)

        if columns is None:
            columns = existing_columns

        # Ensure values is a list of tuples
        if not isinstance(values, list):
            values = [values]
        elif not all(isinstance(v, tuple) for v in values):
            raise ValueError("Values must be a tuple or a list of tuples.")

        for value_tuple in values:
            if len(value_tuple) != len(columns):
                raise ValueError(f"Number of values {len(value_tuple)} does not match number of columns {len(columns)}")

        columns_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"

        try:
            self.db_client.execute_batch_query(query, values)
            logger.info("Records inserted.")
            return True
        except Exception as e:
            logger.error(f"Failed to insert records. Error: {e}")
            raise

    def read(self, table: str, columns: List[str] = None, where: str = "", params: Tuple[Any] = None, show_id: bool = False) -> List[Dict[str, Any]]:
        """
        Read records from the specified table.

        Args:
            table (str): The table name.
            columns (list, optional): List of column names to retrieve. If None, all columns will be retrieved.
            where (str, optional): WHERE clause for filtering records.
            params (tuple, optional): Tuple of parameters for the WHERE clause.
            show_id (bool, optional): If True, include the 'id' column. Default is False.

        Returns:
            list: List of records as dictionaries.
        """
        if columns is None:
            columns = self._get_table_columns(table, show_id=show_id)

        columns_str = ", ".join(columns) if columns else "*"
        query = f"SELECT {columns_str} FROM {table}"
        if where:
            query += f" WHERE {where}"

        try:
            result = self.db_client.execute_query(query, params, fetch_as_dict=True)
            records = [self._format_dates(record) for record in result]
            logger.info("Records found.")
            return records
        except Exception as e:
            logger.error(f"Failed to read records. Error: {e}")
            raise

    def update(self, table: str, updates: Dict[str, Any], where: str, params: Tuple[Any]) -> None:
        """
        Update records in the specified table.

        Args:
            table (str): The table name.
            updates (dict): Dictionary of columns and their new values.
            where (str): WHERE clause for identifying records to update.
            params (tuple): Tuple of parameters for the WHERE clause.
        """
        set_clause = ", ".join([f"{col} = %s" for col in updates.keys()])
        query = f"UPDATE {table} SET {set_clause} WHERE {where}"
        values = tuple(updates.values()) + params
        try:
            self.db_client.execute_query(query, values)
            logger.info("Records updated.")
            return True
        except Exception as e:
            logger.error(f"Failed to update records. Error: {e}")
            raise

    def delete(self, table: str, where: str = "", params: Tuple[Any] = None) -> None:
        """
        Delete records from the specified table.

        Args:
            table (str): The table name.
            where (str, optional): WHERE clause for identifying records to delete. If empty, all records will be deleted.
            params (tuple, optional): Tuple of parameters for the WHERE clause.
        """
        query = f"DELETE FROM {table}"
        if where:
            query += f" WHERE {where}"
        try:
            self.db_client.execute_query(query, params)
            logger.info("Records deleted")
            return True
        except Exception as e:
            logger.error(f"Failed to delete records. Error: {e}")
            raise

    def execute_raw_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> Optional[List[Dict[str, Any]]]:
        """
        Execute a raw SQL query.

        Args:
            query (str): The SQL query to execute.
            params (dict, optional): Dictionary of parameters to bind to the query. Default is None.

        Returns:
            Optional[list]: If the query is a SELECT query, returns a list of dictionaries representing rows with formatted date fields. Otherwise, returns None.
        """
        try:
            is_select_query = query.strip().lower().startswith('select')
            if is_select_query:
                # Execute the SELECT query and get results as a list of dictionaries
                result = self.db_client.execute_query(query, params, fetch_as_dict=True)
                # Format dates in each record
                formatted_result = [self._format_dates(record) for record in result]
                return formatted_result
            else:
                # Execute the non-SELECT query
                self.db_client.execute_query(query, params)
                logger.info("Query executed successfully.")
                return None
        except Exception as e:
            logger.error(f"Failed to execute raw query. Error: {e}")
            raise

