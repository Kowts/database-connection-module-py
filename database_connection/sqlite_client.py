import sqlite3
from .base_database import BaseDatabase, DatabaseConnectionError
import logging

logger = logging.getLogger(__name__)

class SQLiteClient(BaseDatabase):
    """SQLite database connection and operations."""

    def connect(self):
        """Establish the SQLite database connection and create table if it doesn't exist."""
        try:
            self.connection = sqlite3.connect(self.config['database'])
            logger.info("SQLite Database connected successfully.")
            self._create_table_if_not_exists()
        except sqlite3.Error as err:
            logger.error(f"Error connecting to SQLite Database: {err}")
            raise DatabaseConnectionError(err)

    def disconnect(self):
        """Close the SQLite database connection."""
        if self.connection:
            self.connection.close()
            logger.info("SQLite Database disconnected successfully.")

    def execute_query(self, query, params=None):
        """Execute a SQLite database query."""
        cursor = self.connection.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        if query.strip().lower().startswith("select"):
            return cursor.fetchall()
        self.connection.commit()
        return cursor.rowcount

    def _create_table_if_not_exists(self):
        """Create a table if it doesn't exist."""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS test_table (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            age INTEGER NOT NULL
        )
        """
        self.execute_query(create_table_query)
        logger.info("Table 'test_table' ensured to exist.")
