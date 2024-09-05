import logging
from decouple import config
from database_connection import DatabaseFactory, DatabaseConnectionError
from database_connection.postgresql_generic_crud import PostgresqlGenericCRUD
from typing import Any, Dict

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_postgresql_config() -> Dict[str, Any]:
    """
    Retrieve PostgreSQL configuration from environment variables.

    Returns:
        dict: PostgreSQL configuration.
    """
    return {
        'host': config('DB_HOST'),
        'port': config('DB_PORT', cast=int),
        'dbname': config('DB_NAME'),
        'user': config('DB_USER'),
        'password': config('DB_PASSWORD')
    }


# Example usage
if __name__ == "__main__":
    try:
        # PostgreSQL example using environment variables
        postgresql_config = get_postgresql_config()
        postgresql_db = DatabaseFactory.get_database(
            'postgresql', postgresql_config)
        postgresql_db.connect()

        # Initialize PostgresqlGenericCRUD
        crud = PostgresqlGenericCRUD(postgresql_db)

        # Insert a record into the employee_details_ext table
        columns = [
            'number', 'name', 'gender', 'email', 'phone', 'occupation',
            'image', 'company', 'area', 'mail_group', 'birthdate', 'join_date',
            'created_at', 'status'
        ]
        values = (
            'ext00564', 'Miriam Frazão', 'F', 'miriam.frazao@ext.cvt.cv', 5935297,
            'Atendedor Loja', 'ext00564_Miriam Nereida Frazão.JPG', 'CVT', 'CRN',
            'CRN@cvt.cv', '1988-09-13', None, '2024-02-04T13:53:58.190Z', '1'
        )
        crud.create('employee_details_ext', values, columns=columns)

        # Update a record in the employee_details_ext table
        updates = {'email': 'miriam.updated@ext.cvt.cv', 'phone': 123456789}
        where_clause = "number = %s"
        where_params = ('ext00564',)
        crud.update('employee_details_ext', updates,
                    where_clause, where_params)

        # Find all records in the employee_details_ext table
        employees = crud.read('employee_details_ext')
        print("PostgreSQL result:", employees)

        # Delete a single record from the employee_details_ext table
        where_clause = "number = %s"
        where_params = ('ext00564',)
        crud.delete('employee_details_ext', where_clause, where_params)

        # Verify deletion
        employees = crud.read('employee_details_ext')
        print("PostgreSQL result after single deletion:", employees)

        # Insert multiple records for testing
        values_1 = (
            'ext00565', 'John Doe', 'M', 'john.doe@ext.cvt.cv', 5935298,
            'Manager', 'ext00565_John_Doe.JPG', 'CVT', 'HR',
            'HR@cvt.cv', '1985-05-23', None, '2024-02-04T13:53:58.190Z', '1'
        )
        values_2 = (
            'ext00566', 'Jane Doe', 'F', 'jane.doe@ext.cvt.cv', 5935299,
            'Analyst', 'ext00566_Jane_Doe.JPG', 'CVT', 'Finance',
            'Finance@cvt.cv', '1990-11-15', None, '2024-02-04T13:53:58.190Z', '1'
        )
        crud.create('employee_details_ext', values_1, columns=columns)
        crud.create('employee_details_ext', values_2, columns=columns)

        # Delete multiple records from the employee_details_ext table
        where_clause = "number IN (%s, %s)"
        where_params = ('ext00565', 'ext00566')
        crud.delete('employee_details_ext', where_clause, where_params)

        # Verify deletion
        employees = crud.read('employee_details_ext')
        print("PostgreSQL result after multiple deletions:", employees)

        # Insert records for testing delete all
        crud.create('employee_details_ext', values_1, columns=columns)
        crud.create('employee_details_ext', values_2, columns=columns)

        # Delete all records from the employee_details_ext table
        crud.delete('employee_details_ext')

        # Verify deletion
        employees = crud.read('employee_details_ext')
        print("PostgreSQL result after deleting all records:", employees)

        postgresql_db.disconnect()

    except DatabaseConnectionError as e:
        logger.error(f"Failed to connect to database: {e}")
    except ValueError as e:
        logger.error(f"Error: {e}")
