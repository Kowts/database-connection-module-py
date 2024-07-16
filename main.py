# main.py

import logging
from decouple import config
from database_connection import DatabaseFactory, DatabaseConnectionError
from typing import Any, List, Tuple, Dict

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


def insert_employee(db, values: Tuple[Any]) -> None:
    """
    Insert a new employee record into the database.

    Args:
        db: Database client instance.
        values (tuple): Employee details.
    """
    query = """
    INSERT INTO employee_details_ext (
        id, number, name, gender, email, phone, occupation, image, company, area, mail_group, birthdate, join_date, created_at, status
    ) VALUES (
        DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    """
    db.execute_query(query, values)
    logger.info("Inserted employee record.")


def update_employee(db, values: Tuple[Any]) -> None:
    """
    Update an existing employee record in the database.

    Args:
        db: Database client instance.
        values (tuple): Updated employee details.
    """
    query = """
    UPDATE employee_details_ext
    SET email = %s, phone = %s
    WHERE number = %s
    """
    db.execute_query(query, values)
    logger.info("Updated employee record.")


def delete_employee(db, values: Tuple[str]) -> None:
    """
    Delete an employee record from the database.

    Args:
        db: Database client instance.
        values (tuple): Employee number to be deleted.
    """
    query = "DELETE FROM employee_details_ext WHERE number = %s"
    db.execute_query(query, values)
    logger.info("Deleted employee record.")


def fetch_all_employees(db) -> List[Tuple[Any]]:
    """
    Fetch all employee records from the database.

    Args:
        db: Database client instance.

    Returns:
        list: List of employee records.
    """
    query = "SELECT * FROM employee_details_ext"
    return db.execute_query(query)


# Example usage
if __name__ == "__main__":
    try:
        # PostgreSQL example using environment variables
        postgresql_config = get_postgresql_config()
        postgresql_db = DatabaseFactory.get_database('postgresql', postgresql_config)
        postgresql_db.connect()

        # Insert a record into the employee_details_ext table
        insert_values = (
            'ext00564', 'Miriam Frazão', 'F', 'miriam.frazao@ext.cvt.cv', 5935297,
            'Atendedor Loja', 'ext00564_Miriam Nereida Frazão.JPG', 'CVT', 'CRN',
            'CRN@cvt.cv', '1988-09-13', None, '2024-02-04T13:53:58.190Z', '1'
        )
        insert_employee(postgresql_db, insert_values)

        # Update a record in the employee_details_ext table
        update_values = ('miriam.updated@ext.cvt.cv', 123456789, 'ext00564')
        update_employee(postgresql_db, update_values)

        # Find all records in the employee_details_ext table
        employees = fetch_all_employees(postgresql_db)
        print("PostgreSQL result:", employees)

        # Delete a record from the employee_details_ext table
        delete_values = ('ext00456',)
        delete_employee(postgresql_db, delete_values)

        # Verify deletion
        employees = fetch_all_employees(postgresql_db)
        print("PostgreSQL result after deletion:", employees)

        postgresql_db.disconnect()

    except DatabaseConnectionError as e:
        logger.error(f"Failed to connect to database: {e}")
    except ValueError as e:
        logger.error(f"Error: {e}")
