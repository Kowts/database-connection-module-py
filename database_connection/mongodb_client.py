# database_connection/mongodb_database.py
from pymongo import MongoClient
from pymongo.errors import ConfigurationError
from .base_database import BaseDatabase, DatabaseConnectionError
from .ssh_tunnel import SSHTunnelManager
import logging

logger = logging.getLogger(__name__)


class MongoDBClient(BaseDatabase):
    """MongoDB database connection and operations."""

    def __init__(self, config):
        """
        Initialize the MongoDBClient class.

        Args:
            config (dict): Configuration parameters for the MongoDB connection.
        """
        super().__init__(config)
        self.ssh_tunnel_manager = None

    def connect(self):
        """Establish the MongoDB database connection."""
        try:
            # Check if SSH tunneling is required
            if 'ssh' in self.config:
                ssh_config = self.config.pop('ssh')
                self.ssh_tunnel_manager = SSHTunnelManager(
                    ssh_host=ssh_config['ssh_host'],
                    ssh_user=ssh_config['ssh_user'],
                    ssh_pass=ssh_config['ssh_pass'],
                    ssh_port=ssh_config['ssh_port'],
                    remote_host=self.config['host'],
                    remote_port=self.config['port']
                )
                local_bind_port = self.ssh_tunnel_manager.create_ssh_tunnel()
                self.config['host'] = '127.0.0.1'
                self.config['port'] = local_bind_port

            self.database_name = self.config.pop('database')
            self.collection_name = self.config.pop('collection')

            self.connection = MongoClient(
                host=self.config['host'],
                port=self.config['port'],
                username=self.config.get('username'),
                password=self.config.get('password'),
                authSource=self.config.get('authSource', 'admin')
            )
            self.database = self.connection[self.database_name]
            self.collection = self.database[self.collection_name]
            logger.info("MongoDB Database connected successfully.")
        except ConfigurationError as err:
            logger.error(f"Error connecting to MongoDB Database: {err}")
            raise DatabaseConnectionError(err)

    def disconnect(self):
        """Close the MongoDB database connection."""
        if self.connection:
            self.connection.close()
            logger.info("MongoDB Database disconnected successfully.")
        if self.ssh_tunnel_manager:
            self.ssh_tunnel_manager.close()

    def execute_query(self, query, params=None):
        """
        Execute a MongoDB database operation.

        Args:
            query (dict): The query operation to be performed.
            params (dict, optional): Parameters for the query.

        Returns:
            Result of the query execution.

        Raises:
            ValueError: If the query operation is unsupported.
        """
        if 'find' in query:
            return list(self.collection.find(query['find']))
        elif 'insert' in query:
            return self.collection.insert_one(query['insert'])
        elif 'update' in query:
            return self.collection.update_one(query['update']['filter'], query['update']['update'])
        elif 'delete' in query:
            return self.collection.delete_one(query['delete'])
        else:
            raise ValueError("Unsupported query operation.")
