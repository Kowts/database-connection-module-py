
import cx_Oracle
from .base_database import BaseDatabase, DatabaseConnectionError
import logging

logger = logging.getLogger(__name__)

class OracleClient(BaseDatabase):
    """Oracle database connection and operations."""

    def connect(self):
        """Establish the Oracle database connection."""
        try:
            # Initialize Oracle client if necessary
            cx_Oracle.init_oracle_client(lib_dir=r"C:\Program Files\PremiumSoft\Navicat Premium 16\instantclient_11_2")

            # Use service_name or sid based on configuration
            if 'service_name' in self.config:
                dsn = cx_Oracle.makedsn(
                    self.config['host'], self.config['port'], service_name=self.config['service_name'])
            elif 'sid' in self.config:
                dsn = cx_Oracle.makedsn(
                    self.config['host'], self.config['port'], sid=self.config['sid'])
            else:
                raise ValueError(
                    "Either 'service_name' or 'sid' must be provided in the configuration.")

            self.connection = cx_Oracle.connect(
                self.config['user'], self.config['password'], dsn)
            logger.info("Oracle Database connected successfully.")
        except cx_Oracle.Error as err:
            error, = err.args
            logger.error(
                f"Error connecting to Oracle Database: {error.message}")
            raise DatabaseConnectionError(error.message)

    def disconnect(self):
        """Close the Oracle database connection."""
        if self.connection:
            self.connection.close()
            logger.info("Oracle Database disconnected successfully.")

    def execute_query(self, query, params=None):
        """Execute an Oracle database query."""
        cursor = self.connection.cursor()
        cursor.execute(query, params or {})
        if query.strip().lower().startswith("select"):
            return cursor.fetchall()
        self.connection.commit()
        return cursor.rowcount
