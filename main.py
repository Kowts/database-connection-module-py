import logging
from database_connection import DatabaseFactory, DatabaseConnectionError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Example usage
if __name__ == "__main__":
    try:
        # MongoDB example with SSH tunnel
        mongodb_config_with_ssh = {
            'host': '127.0.0.1',
            'port': 27017,
            'username': 'admin',
            'password': 'dsi_admin_Pass123',
            'database': 'mobiledata_db',
            'collection': 'notifications',
            'ssh': {
                'ssh_host': '10.16.22.72',
                'ssh_user': 'dsi_superuser',
                'ssh_pass': '123qwe_super',
                'ssh_port': 22
            }
        }
        mongodb_db_with_ssh = DatabaseFactory.get_database('mongodb', mongodb_config_with_ssh)
        mongodb_db_with_ssh.connect()
        mongodb_result_with_ssh = mongodb_db_with_ssh.execute_query({'find': {'type': "unified-format"}})
        print("MongoDB result with SSH:", mongodb_result_with_ssh)
        mongodb_db_with_ssh.disconnect()


    except DatabaseConnectionError as e:
        logger.error(f"Failed to connect to database: {e}")
    except ValueError as e:
        logger.error(f"Error: {e}")
