from typing import Any, Dict, List, Optional, Tuple
import logging
from datetime import date, datetime
from utils import retry
import re
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class MySQLGenericCRUD:
    """Generic CRUD operations for any MySQL table with enhanced functionality."""

    def __init__(self, db_client):
        """
        Initialize the MySQLGenericCRUD class.

        Args:
            db_client: An instance of a database client (e.g., MySQLClient).
        """
        self.db_client = db_client

    def _validate_table_name(self, table_name: str) -> bool:
        """
        Validate that a table name contains only allowed characters.

        Args:
            table_name (str): The table name to validate.

        Returns:
            bool: True if the table name is valid, False otherwise.
        """
        pattern = re.compile(r'^[A-Za-z0-9_]+$')
        return bool(pattern.match(table_name))

    def _get_table_columns(self, table: str, show_id: bool = False) -> List[str]:
        """
        Get the column names of a table with enhanced metadata.

        Args:
            table (str): The table name.
            show_id (bool): If True, include the 'id' column.

        Returns:
            list: List of column names.
        """
        query = """
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
        AND TABLE_NAME = %s
        """

        if not show_id:
            query += "AND COLUMN_NAME != 'id' "
        query += "ORDER BY ORDINAL_POSITION"

        try:
            result = self.db_client.execute_query(query, (table,), fetch_as_dict=True)
            columns = [row['COLUMN_NAME'] for row in result]
            return columns
        except Exception as e:
            logger.error(f"Failed to get table columns. Error: {e}")
            raise

    def _infer_column_types(self, values: List[Tuple[Any]], columns: List[str], primary_key: str = None) -> Dict[str, str]:
        """
        Infer MySQL column types based on the provided values.

        Args:
            values: List of tuples containing the values.
            columns: List of column names.
            primary_key: Optional primary key column name.

        Returns:
            dict: Mapping of column names to MySQL data types.
        """
        type_mapping = {
            int: "INT",
            float: "FLOAT",
            str: "VARCHAR(255)",
            date: "DATE",
            datetime: "DATETIME",
            bool: "BOOLEAN"
        }

        inferred_types = {}
        for idx, column in enumerate(columns):
            # Get all non-None values for this column
            column_values = [row[idx] for row in values if row[idx] is not None]

            if not column_values:
                inferred_types[column] = "VARCHAR(255)"
                continue

            sample_value = column_values[0]
            python_type = type(sample_value)

            # Check if all values of the same type have the same Python type
            if all(isinstance(val, python_type) for val in column_values):
                if python_type is str:
                    # Calculate max length for VARCHAR
                    max_length = max(len(str(val)) for val in column_values)
                    max_length = min(max_length * 2, 65535)  # Add some padding but respect MySQL limits
                    inferred_types[column] = f"VARCHAR({max_length})"
                else:
                    inferred_types[column] = type_mapping.get(python_type, "VARCHAR(255)")
            else:
                inferred_types[column] = "VARCHAR(255)"

            # Add primary key constraint if specified
            if column == primary_key:
                inferred_types[column] += " PRIMARY KEY"

        return inferred_types

    def create_table_if_not_exists(self, table: str, columns: List[str], values: List[Tuple[Any]], primary_key: str = None) -> None:
        """
        Create a table if it doesn't exist with enhanced schema management.

        Args:
            table: Table name.
            columns: List of column names.
            values: Sample values for type inference.
            primary_key: Optional primary key column.
        """
        try:
            if not self._validate_table_name(table):
                raise ValueError(f"Invalid table name: {table}")

            # Check if table exists
            check_query = """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = DATABASE()
            AND table_name = %s
            """
            result = self.db_client.execute_query(check_query, (table,))
            if result[0][0] > 0:
                logger.info(f"Table '{table}' already exists.")
                return

            # Add primary key if specified
            if primary_key and primary_key not in columns:
                columns.append(primary_key)

            # Infer column types
            column_types = self._infer_column_types(values, columns, primary_key)
            columns_def = ", ".join([f"{col} {dtype}" for col, dtype in column_types.items()])

            # Create table with proper engine and character set
            create_query = f"""
            CREATE TABLE {table} (
                {columns_def}
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """

            self.db_client.execute_query(create_query)
            logger.info(f"Table '{table}' created successfully.")
        except Exception as e:
            logger.error(f"Failed to create table '{table}'. Error: {e}")
            raise

    def _format_dates(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Format date fields in a record."""
        for key, value in record.items():
            if isinstance(value, (date, datetime)):
                record[key] = value.strftime('%Y-%m-%d %H:%M:%S') if isinstance(value, datetime) else value.strftime('%Y-%m-%d')
        return record

    @retry(max_retries=5, delay=5, backoff=2, exceptions=(Exception,), logger=logger)
    def create(self, table: str, values: List[Tuple[Any]], columns: List[str] = None, primary_key: str = None) -> bool:
        """
        Create new records in the specified table with enhanced error handling and retry mechanism.

        Args:
            table: Table name.
            values: List of value tuples to insert.
            columns: Optional list of column names.
            primary_key: Optional primary key column.

        Returns:
            bool: Success status of the operation.
        """
        if columns is None:
            columns = self._get_table_columns(table)

        # Ensure values is properly formatted
        if not isinstance(values, list):
            values = [values]
        elif not all(isinstance(v, tuple) for v in values):
            values = [tuple(v) if not isinstance(v, tuple) else v for v in values]

        # Create table if it doesn't exist
        self.create_table_if_not_exists(table, columns, values, primary_key)

        # Validate value lengths
        for value_tuple in values:
            if len(value_tuple) != len(columns):
                raise ValueError(f"Number of values {len(value_tuple)} does not match number of columns {len(columns)}")

        # Prepare and execute the insert query
        columns_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"

        try:
            self.db_client.execute_batch_query(query, values)
            logger.info(f"Successfully inserted {len(values)} records into {table}")
            return True
        except Exception as e:
            logger.error(f"Failed to insert records into {table}. Error: {e}")
            return False

    @retry(max_retries=5, delay=5, backoff=2, exceptions=(Exception,), logger=logger)
    def read(self, table: str, columns: List[str] = None, where: str = "",
            params: Tuple[Any] = None, show_id: bool = False,
            batch_size: int = None, order_by: str = None) -> List[Dict[str, Any]]:
        """
        Read records from the specified table with enhanced querying capabilities.

        Args:
            table: Table name.
            columns: Optional list of columns to retrieve.
            where: Optional WHERE clause.
            params: Optional query parameters.
            show_id: Include ID column flag.
            batch_size: Optional batch size for pagination.
            order_by: Optional ORDER BY clause.

        Returns:
            list: List of records as dictionaries.
        """
        if columns is None:
            columns = self._get_table_columns(table, show_id=show_id)

        columns_str = ", ".join(columns)
        query = f"SELECT {columns_str} FROM {table}"

        if where:
            query += f" WHERE {where}"

        if order_by:
            query += f" ORDER BY {order_by}"

        if batch_size:
            query += f" LIMIT {batch_size}"

        try:
            result = self.db_client.execute_query(query, params, fetch_as_dict=True)
            records = [self._format_dates(record) for record in result]
            logger.info(f"Successfully retrieved {len(records)} records from {table}")
            return records
        except Exception as e:
            logger.error(f"Failed to read records from {table}. Error: {e}")
            raise

    @retry(max_retries=5, delay=5, backoff=2, exceptions=(Exception,), logger=logger)
    def update(self, table: str, updates: Dict[str, Any], where: str,
              params: Tuple[Any], batch_size: int = None) -> bool:
        """
        Update records in the specified table with enhanced batch processing.

        Args:
            table: Table name.
            updates: Dictionary of column-value pairs to update.
            where: WHERE clause for identifying records.
            params: Query parameters.
            batch_size: Optional batch size for large updates.

        Returns:
            bool: Success status of the operation.
        """
        set_clause = ", ".join([f"{col} = %s" for col in updates.keys()])
        query = f"UPDATE {table} SET {set_clause} WHERE {where}"

        if batch_size:
            query += f" LIMIT {batch_size}"

        values = tuple(updates.values()) + params

        try:
            self.db_client.execute_query(query, values)
            logger.info(f"Successfully updated records in {table}")
            return True
        except Exception as e:
            logger.error(f"Failed to update records in {table}. Error: {e}")
            return False

    @retry(max_retries=5, delay=5, backoff=2, exceptions=(Exception,), logger=logger)
    def delete(self, table: str, where: str = "", params: Tuple[Any] = None,
              batch_size: int = None, safe_delete: bool = True) -> bool:
        """
        Delete records from the specified table with enhanced safety features.

        Args:
            table: Table name.
            where: Optional WHERE clause.
            params: Optional query parameters.
            batch_size: Optional batch size for large deletes.
            safe_delete: If True, requires WHERE clause for deletion.

        Returns:
            bool: Success status of the operation.
        """
        if safe_delete and not where:
            raise ValueError("WHERE clause is required when safe_delete is enabled")

        query = f"DELETE FROM {table}"
        if where:
            query += f" WHERE {where}"
        if batch_size:
            query += f" LIMIT {batch_size}"

        try:
            self.db_client.execute_query(query, params)
            logger.info(f"Successfully deleted records from {table}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete records from {table}. Error: {e}")
            return False

    def execute_raw_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> Optional[List[Dict[str, Any]]]:
        """
        Execute a raw SQL query with enhanced safety and result handling.

        Args:
            query: SQL query string.
            params: Optional query parameters.

        Returns:
            Optional[List[Dict[str, Any]]]: Query results if applicable.
        """
        try:
            is_select = query.strip().lower().startswith('select')
            result = self.db_client.execute_query(query, params, fetch_as_dict=is_select)

            if is_select:
                return [self._format_dates(record) for record in result]
            else:
                logger.info("Query executed successfully.")
                return None
        except Exception as e:
            logger.error(f"Failed to execute raw query. Error: {e}")
            raise

    def begin_transaction(self):
        """Begin a database transaction."""
        try:
            self.db_client.execute_query("START TRANSACTION")
            logger.info("Transaction started")
        except Exception as e:
            logger.error(f"Failed to start transaction: {e}")
            raise

    def commit_transaction(self):
        """Commit the current transaction."""
        try:
            self.db_client.execute_query("COMMIT")
            logger.info("Transaction committed")
        except Exception as e:
            logger.error(f"Failed to commit transaction: {e}")
            raise

    def rollback_transaction(self):
        """Rollback the current transaction."""
        try:
            self.db_client.execute_query("ROLLBACK")
            logger.info("Transaction rolled back")
        except Exception as e:
            logger.error(f"Failed to rollback transaction: {e}")
            raise

    @contextmanager
    def transaction(self, savepoint_name: str = None):
        """
        Context manager for handling transactions with optional savepoint support.

        Args:
            savepoint_name (str, optional): Name of the savepoint if using nested transactions.

        Usage:
            # Simple transaction
            with crud.transaction():
                crud.create(...)
                crud.update(...)

            # Nested transaction with savepoint
            with crud.transaction():
                crud.create(...)
                with crud.transaction('my_savepoint'):
                    crud.update(...)  # If this fails, only this part is rolled back
                crud.delete(...)      # This will still execute

        Raises:
            Any exception that occurs during the transaction.
        """
        try:
            if savepoint_name:
                # Create a savepoint for nested transaction
                self.db_client.execute_query(f"SAVEPOINT {savepoint_name}")
                logger.info(f"Created savepoint: {savepoint_name}")
            else:
                # Start a new transaction
                self.begin_transaction()

            # Yield control to the caller
            yield

            # If we reach here, commit the transaction
            if savepoint_name:
                # Release the savepoint if successful
                self.db_client.execute_query(f"RELEASE SAVEPOINT {savepoint_name}")
                logger.info(f"Released savepoint: {savepoint_name}")
            else:
                self.commit_transaction()

        except Exception as e:
            # Handle any errors that occur during the transaction
            if savepoint_name:
                # Rollback to the savepoint if it exists
                self.db_client.execute_query(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                logger.error(f"Rolled back to savepoint {savepoint_name}: {str(e)}")
            else:
                # Rollback the entire transaction
                self.rollback_transaction()
                logger.error(f"Transaction rolled back due to error: {str(e)}")
            raise  # Re-raise the exception after rollback
