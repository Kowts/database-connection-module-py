from .mysql_client import MySQLClient
from .postgresql_client import PostgreSQLClient
from .mongodb_client import MongoDBClient
from .sqlite_client import SQLiteClient
from .sqlserver_client import SQLServerClient
from .oracle_client import OracleClient

class DatabaseFactory:
    """
    Factory class to get the appropriate database instance.

    Methods:
        get_database(db_type, config): Returns an instance of the requested database type.
    """

    @staticmethod
    def get_database(db_type, config):
        """
        Get the database instance based on the db_type.

        Args:
            db_type (str): The type of the database ('mysql', 'postgresql', 'mongodb', 'sqlite', 'sqlserver', 'oracle').
            config (dict): Configuration parameters for the database connection.

        Returns:
            BaseDatabase: An instance of the appropriate database subclass.
        """
        if db_type == 'mysql':
            return MySQLClient(config)
        elif db_type == 'postgresql':
            return PostgreSQLClient(config)
        elif db_type == 'mongodb':
            return MongoDBClient(config)
        elif db_type == 'sqlite':
            return SQLiteClient(config)
        elif db_type == 'sqlserver':
            return SQLServerClient(config)
        elif db_type == 'oracle':
            return OracleClient(config)
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
