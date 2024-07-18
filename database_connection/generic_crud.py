from typing import Any, Dict, List, Tuple
import logging

logger = logging.getLogger(__name__)


class GenericCRUD:
    """Generic CRUD operations for any table."""

    def __init__(self, db_client):
        """
        Initialize the GenericCRUD class.

        Args:
            db_client: An instance of a database client (e.g., PostgreSQLClient).
        """
        self.db_client = db_client

    def _get_table_columns(self, table: str) -> List[str]:
        """
        Get the column names of a table, excluding auto-increment and default columns.

        Args:
            table (str): The table name.

        Returns:
            list: List of column names.
        """
        query = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = %s
        AND (column_default IS NULL OR column_default LIKE 'nextval%%')
        ORDER BY ordinal_position
        """
        try:
            result = self.db_client.execute_query(
                query, (table,), fetch_as_dict=True)
            columns = [row['column_name'] for row in result]
            return columns
        except Exception as e:
            logger.error(f"Failed to get table columns. Error: {e}")
            raise

    def create(self, table: str, values: Any, columns: List[str] = None) -> None:
        """
        Create new records in the specified table.

        Args:
            table (str): The table name.
            values (Any): Tuple or list of tuples of values to insert.
            columns (list, optional): List of column names. If None, columns will be inferred from the table schema.
        """
        # Ensure values is a list of tuples
        if not isinstance(values, list):
            values = [values]
        elif not all(isinstance(v, tuple) for v in values):
            raise ValueError("Values must be a tuple or a list of tuples.")

        if columns is None:
            columns = self._get_table_columns(table)

        # Check if the number of values matches the number of columns
        for value_tuple in values:
            if len(value_tuple) != len(columns):
                raise ValueError(
                    f"Number of values {len(value_tuple)} does not match number of columns {len(columns)}")

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

    def read(self, table: str, columns: List[str] = None, where: str = "", params: Tuple[Any] = None) -> List[Dict[str, Any]]:
        """
        Read records from the specified table.

        Args:
            table (str): The table name.
            columns (list, optional): List of column names to retrieve. If None, all columns will be retrieved.
            where (str, optional): WHERE clause for filtering records.
            params (tuple, optional): Tuple of parameters for the WHERE clause.

        Returns:
            list: List of records as dictionaries.
        """
        columns_str = ", ".join(columns) if columns else "*"
        query = f"SELECT {columns_str} FROM {table}"
        if where:
            query += f" WHERE {where}"
        try:
            result = self.db_client.execute_query(
                query, params, fetch_as_dict=True)
            logger.info("Records found.")
            return result
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
