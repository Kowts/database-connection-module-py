# Database Connection Module

This project provides a modular and extensible way to manage database connections and operations for various types of databases. It includes support for SSH tunneling to secure database connections when needed.

### Modules Description

#### `base_database.py`

Abstract base class for database operations, defining the interface that all database classes must implement.

#### `database_factory.py`

Factory class to get the appropriate database instance based on the provided database type and configuration.

#### `ssh_tunnel.py`

Manages SSH tunneling for database connections to enhance security.

#### `mysql_client.py`, `postgresql_client.py`, `sqlite_client.py`, `sqlserver_client.py`, `oracle_client.py`

These modules handle connections and operations for MySQL, PostgreSQL, SQLite, SQL Server, and Oracle databases, respectively.

## How to Use and Configuration
Check the "HOW TO USE" and `main.py` file for detailed instructions on how to use the database connection module.

## Requirements
- Python 3.6+
- `ssh_tunnel` library for SSH tunneling
- `mysql-connector-python`, `psycopg2`, `pyodbc`, `cx_Oracle`, `sqlite3` libraries for MySQL, PostgreSQL, SQL Server, Oracle and SQLite databases

## Notes
- Ensure the appropriate database drivers are installed.
- Replace placeholder values (e.g., your_username, your_password, ssh.example.com) with actual values.

## License
This project is licensed under the MIT License.
