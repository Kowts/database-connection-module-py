from .base_database import BaseDatabase, DatabaseConnectionError
import logging
import mysql.connector

logger = logging.getLogger(__name__)

class MySQLClient(BaseDatabase):
    """MySQL database connection and operations."""

    def connect(self):
        """Establish the MySQL database connection."""
        try:
            self.connection = mysql.connector.connect(**self.config)
            logger.info("MySQL Database connected successfully.")
        except mysql.connector.Error as err:
            logger.error(f"Error connecting to MySQL Database: {err}")
            raise DatabaseConnectionError(err)

    def disconnect(self):
        """Close the MySQL database connection."""
        if self.connection:
            self.connection.close()
            logger.info("MySQL Database disconnected successfully.")

    def execute_query(self, query, params=None):
        """Execute a MySQL database query."""
        cursor = self.connection.cursor()
        cursor.execute(query, params)
        if query.strip().lower().startswith("select"):
            return cursor.fetchall()
        self.connection.commit()
        return cursor.rowcount
