import psycopg2
from .base_database import BaseDatabase, DatabaseConnectionError
import logging

logger = logging.getLogger(__name__)

class PostgreSQLClient(BaseDatabase):
    """PostgreSQL database connection and operations."""

    def connect(self):
        """Establish the PostgreSQL database connection."""
        try:
            self.connection = psycopg2.connect(**self.config)
            logger.info("PostgreSQL Database connected successfully.")
        except psycopg2.Error as err:
            logger.error(f"Error connecting to PostgreSQL Database: {err}")
            raise DatabaseConnectionError(err)

    def disconnect(self):
        """Close the PostgreSQL database connection."""
        if self.connection:
            self.connection.close()
            logger.info("PostgreSQL Database disconnected successfully.")

    def execute_query(self, query, params=None):
        """Execute a PostgreSQL database query."""
        cursor = self.connection.cursor()
        cursor.execute(query, params)
        if query.strip().lower().startswith("select"):
            return cursor.fetchall()
        self.connection.commit()
        return cursor.rowcount
