from typing import Any, Dict, List, Optional, Tuple
import logging
from datetime import date, datetime
from tqdm import tqdm
from utils import retry
import re

logger = logging.getLogger(__name__)

class SQLServerGenericCRUD:
    """Generic CRUD operations for any table in SQL Server with enhanced functionality."""

    def __init__(self, db_client):
        """
        Initialize the SQLServerGenericCRUD class.

        Args:
            db_client: An instance of SQLServerClient.
        """
        self.db_client = db_client

    def _get_table_columns(self, table: str, show_id: bool = False) -> List[str]:
        """
        Get the column names of a table with improved metadata handling.

        Args:
            table (str): The table name.
            show_id (bool): If True, include the 'id' column.

        Returns:
            list: List of column names.
        """
        query = """
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = ?
        """
        if not show_id:
            query += " AND COLUMN_NAME != 'ID'"
        query += " ORDER BY ORDINAL_POSITION"

        try:
            result = self.db_client.execute_query(query, (table,), fetch_as_dict=True)
            return [row['COLUMN_NAME'] for row in result]
        except Exception as e:
            logger.error(f"Failed to get table columns: {e}")
            raise

    def _validate_table_name(self, table_name: str) -> bool:
        """
        Validate table name against SQL injection and naming rules.

        Args:
            table_name (str): The table name to validate.

        Returns:
            bool: True if valid, False otherwise.
        """
        pattern = re.compile(r'^[A-Za-z][A-Za-z0-9_]*$')
        return bool(pattern.match(table_name))

    def _format_dates(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format date fields in a record with timezone handling.

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
        Infer SQL Server-specific column types with improved type mapping.

        Args:
            values (list of tuples): Sample data for type inference.
            columns (list): Column names.
            primary_key (str, optional): Primary key column name.

        Returns:
            dict: Mapping of columns to SQL Server data types.
        """
        type_mapping = {
            int: "INT",
            float: "FLOAT",
            str: "NVARCHAR(MAX)",
            date: "DATE",
            datetime: "DATETIME2",
            bool: "BIT",
            bytes: "VARBINARY(MAX)"
        }

        inferred_types = {}
        for idx, column in enumerate(columns):
            # Sample multiple rows for better type inference
            sample_values = [row[idx] for row in values if row[idx] is not None]
            if not sample_values:
                inferred_types[column] = "NVARCHAR(MAX)"
                continue

            # Determine type based on all non-null values
            python_type = type(sample_values[0])
            for value in sample_values[1:]:
                if type(value) != python_type:
                    python_type = str  # Default to string for mixed types
                    break

            sql_type = type_mapping.get(python_type, "NVARCHAR(MAX)")

            # Add primary key constraint if applicable
            if column == primary_key:
                sql_type += " PRIMARY KEY"

            inferred_types[column] = sql_type

        return inferred_types

    def create_table_if_not_exists(self, table: str, columns: List[str],
                                 values: List[Tuple[Any]], primary_key: str = None) -> None:
        """
        Create a table with improved schema handling and constraints.

        Args:
            table (str): The table name.
            columns (list): Column names.
            values (list of tuples): Sample data for type inference.
            primary_key (str, optional): Primary key column name.
        """
        if not self._validate_table_name(table):
            raise ValueError(f"Invalid table name: {table}")

        check_query = """
        SELECT COUNT(*) AS table_exists
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME = ?
        """

        result = self.db_client.execute_query(check_query, (table,), fetch_as_dict=True)
        if result[0]['table_exists'] > 0:
            logger.info(f"Table '{table}' already exists.")
            return

        try:
            column_types = self._infer_column_types(values, columns, primary_key)
            columns_def = ", ".join([f"[{col}] {dtype}" for col, dtype in column_types.items()])

            create_query = f"""
            CREATE TABLE [{table}] (
                {columns_def}
            )
            """

            self.db_client.execute_query(create_query)
            logger.info(f"Table '{table}' created successfully.")

        except Exception as e:
            logger.error(f"Failed to create table '{table}': {e}")
            raise

    @retry(max_retries=5, delay=5, backoff=2, exceptions=(Exception,), logger=logger)
    def create(self, table: str, values: List[Tuple[Any]], columns: List[str] = None,
               primary_key: str = None, batch_size: int = 1000) -> bool:
        """
        Create new records with progress bar visualization.

        Args:
            table (str): The table name.
            values (list of tuples): Values to insert.
            columns (list, optional): Column names.
            primary_key (str, optional): Primary key column name.
            batch_size (int): Size of each batch for processing.

        Returns:
            bool: True if successful, False otherwise.
        """
        if columns is None:
            columns = self._get_table_columns(table)

        # Ensure values is properly formatted
        if not isinstance(values, list):
            values = [values]
        values = [tuple(v) if not isinstance(v, tuple) else v for v in values]

        # Validate data
        for value_tuple in values:
            if len(value_tuple) != len(columns):
                raise ValueError(f"Number of values {len(value_tuple)} does not match number of columns {len(columns)}")

        # Create table if it doesn't exist
        self.create_table_if_not_exists(table, columns, values, primary_key)

        # Prepare the insert query
        columns_str = ", ".join([f"[{col}]" for col in columns])
        placeholders = ", ".join(["?" for _ in columns])
        query = f"INSERT INTO [{table}] ({columns_str}) VALUES ({placeholders})"

        try:
            total_batches = (len(values) - 1) // batch_size + 1

            with tqdm(total=len(values), desc="Inserting records", unit="records") as pbar:
                for i in range(0, len(values), batch_size):
                    batch = values[i:i + batch_size]
                    self.db_client.execute_batch_query(query, batch)
                    pbar.update(len(batch))

            return True
        except Exception as e:
            logger.error(f"Failed to insert records: {e}")
            return False

    @retry(max_retries=5, delay=5, backoff=2, exceptions=(Exception,), logger=logger)
    def read(self, table: str, columns: List[str] = None, where: str = "",
            params: Tuple[Any] = None, show_id: bool = False,
            batch_size: Optional[int] = None, order_by: str = None) -> List[Dict[str, Any]]:
        """
        Read records with improved filtering and pagination.

        Args:
            table (str): The table name.
            columns (list, optional): Column names to retrieve.
            where (str, optional): WHERE clause.
            params (tuple, optional): Query parameters.
            show_id (bool, optional): Include ID column.
            batch_size (int, optional): Number of records per batch.
            order_by (str, optional): ORDER BY clause.

        Returns:
            list: List of records as dictionaries.
        """
        if not self._validate_table_name(table):
            raise ValueError(f"Invalid table name: {table}")

        if columns is None:
            columns = self._get_table_columns(table, show_id=show_id)

        columns_str = ", ".join([f"[{col}]" for col in columns])
        query = f"SELECT {columns_str} FROM [{table}]"

        if where:
            query += f" WHERE {where}"

        if order_by:
            query += f" ORDER BY {order_by}"
        elif batch_size:
            # Need an ORDER BY clause for OFFSET/FETCH
            query += f" ORDER BY {columns[0]}"

        if batch_size:
            query += f" OFFSET 0 ROWS FETCH NEXT {batch_size} ROWS ONLY"

        try:
            result = self.db_client.execute_query(query, params, fetch_as_dict=True)
            records = [self._format_dates(record) for record in result]
            logger.info(f"Retrieved {len(records)} records")
            return records
        except Exception as e:
            logger.error(f"Failed to read records: {e}")
            raise

    @retry(max_retries=5, delay=5, backoff=2, exceptions=(Exception,), logger=logger)
    def update(self, table: str, updates: Dict[str, Any], where: str,
              params: Tuple[Any], batch_size: Optional[int] = None) -> bool:
        """
        Update records with improved batching and validation.

        Args:
            table (str): The table name.
            updates (dict): Column-value pairs to update.
            where (str): WHERE clause.
            params (tuple): Query parameters.
            batch_size (int, optional): Batch size for large updates.

        Returns:
            bool: True if successful, False otherwise.
        """
        if not self._validate_table_name(table):
            raise ValueError(f"Invalid table name: {table}")

        set_clause = ", ".join([f"[{col}] = ?" for col in updates.keys()])
        query = f"UPDATE [{table}] SET {set_clause} WHERE {where}"

        if batch_size:
            query += f" OFFSET 0 ROWS FETCH NEXT {batch_size} ROWS ONLY"

        values = tuple(updates.values()) + params

        try:
            affected_rows = self.db_client.execute_query(query, values)
            logger.info(f"Updated {affected_rows} records")
            return True
        except Exception as e:
            logger.error(f"Failed to update records: {e}")
            return False

    @retry(max_retries=5, delay=5, backoff=2, exceptions=(Exception,), logger=logger)
    def delete(self, table: str, where: str = "", params: Tuple[Any] = None,
              batch_size: Optional[int] = None, safe_delete: bool = True) -> bool:
        """
        Delete records with improved safety and batching.

        Args:
            table (str): The table name.
            where (str, optional): WHERE clause.
            params (tuple, optional): Query parameters.
            batch_size (int, optional): Batch size for large deletes.
            safe_delete (bool): If True, requires WHERE clause for deletion.

        Returns:
            bool: True if successful, False otherwise.
        """
        if not self._validate_table_name(table):
            raise ValueError(f"Invalid table name: {table}")

        if safe_delete and not where:
            raise ValueError("WHERE clause required for safe delete operation")

        query = f"DELETE FROM [{table}]"
        if where:
            query += f" WHERE {where}"

        if batch_size:
            query += f" OFFSET 0 ROWS FETCH NEXT {batch_size} ROWS ONLY"

        try:
            affected_rows = self.db_client.execute_query(query, params)
            logger.info(f"Deleted {affected_rows} records")
            return True
        except Exception as e:
            logger.error(f"Failed to delete records: {e}")
            return False

    def execute_raw_query(self, query: str, params: Optional[Dict[str, Any]] = None,
                         fetch_as_dict: bool = True) -> Optional[List[Dict[str, Any]]]:
        """
        Execute a raw SQL query with improved safety and result handling.

        Args:
            query (str): The SQL query.
            params (dict, optional): Query parameters.
            fetch_as_dict (bool): Return results as dictionaries.

        Returns:
            Optional[list]: Query results or None for non-SELECT queries.
        """
        try:
            is_select = query.strip().lower().startswith('select')
            result = self.db_client.execute_query(query, params, fetch_as_dict=fetch_as_dict)

            if is_select and fetch_as_dict:
                return [self._format_dates(record) for record in result]
            return result
        except Exception as e:
            logger.error(f"Failed to execute raw query: {e}")
            raise

    def table_exists(self, table: str) -> bool:
        """
        Check if a table exists.

        Args:
            table (str): The table name.

        Returns:
            bool: True if table exists, False otherwise.
        """
        query = """
        SELECT COUNT(*) AS table_exists
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME = ?
        """
        try:
            result = self.db_client.execute_query(query, (table,), fetch_as_dict=True)
            return result[0]['table_exists'] > 0
        except Exception as e:
            logger.error(f"Failed to check table existence: {e}")
            raise
