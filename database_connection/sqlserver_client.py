import pyodbc
from .base_database import BaseDatabase, DatabaseConnectionError
import logging

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
                f"{self.config.get('additional_params', '')}"
            )
            self.connection = pyodbc.connect(connection_string)
            logger.info("SQL Server Database connected successfully.")
        except pyodbc.Error as err:
            logger.error(f"Error connecting to SQL Server Database: {err}")
            raise DatabaseConnectionError(err)

    def disconnect(self):
        """Close the SQL Server database connection."""
        if self.connection:
            self.connection.close()
            logger.info("SQL Server Database disconnected successfully.")

    def execute_query(self, query, params=None):
        """Execute a SQL Server database query."""
        cursor = self.connection.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        if query.strip().lower().startswith("select"):
            return cursor.fetchall()
        self.connection.commit()
        return cursor.rowcount
