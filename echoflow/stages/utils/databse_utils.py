"""
This module contains utility functions for working with SQLite databases and managing log data.

The functions in this module provide an interface for creating, updating, and retrieving log data
in an SQLite database. The module also includes functions to manage database connections and perform
SQL queries.

Functions:
    get_connection(path: str='') -> sqlite3.Connection:
        Gets a SQLite database connection.

    execute_sql(conn: sqlite3.Connection, query: str) -> Any:
        Executes an SQL query on the provided connection.

    create_table(conn: sqlite3.Connection, table_name: str, columns: List[str]):
        Creates a table in the database.

    create_log_table(conn: sqlite3.Connection):
        Creates a 'log' table in the database.

    insert_log_data_by_conn(conn: sqlite3.Connection, log: DB_Log) -> int:
        Inserts log data into the 'log' table using the provided connection.

    insert_log_data_by_path(path: str, log: DB_Log) -> int:
        Inserts log data into the 'log' table using the database path.

    convert_to_serializable_dict(obj: Any) -> Union[BaseModel, dict, List[dict], Any]:
        Converts objects to a serializable dictionary format.

    update_log_data_by_conn(conn: sqlite3.Connection, log: DB_Log):
        Updates log data in the 'log' table using the provided connection.

    update_log_data_by_path(path: str, log: DB_Log):
        Updates log data in the 'log' table using the database path.

    parse_all_log_data(conn: sqlite3.Connection) -> List[DB_Log]:
        Parses and retrieves all log data from the 'log' table.

    get_last_log(conn: sqlite3.Connection) -> DB_Log:
        Retrieves the most recent log entry from the 'log' table.

    parse_log(row: Tuple) -> DB_Log:
        Parses a row of log data and creates a DB_Log object.

Classes:
    DB_Log:
        Data class representing a log entry.

    Log_Data:
        Data class representing detailed log information.

Notes:
    - The module assumes the existence of a database with the 'log' table for storing log data.
    - The functions provided here can be used to interact with the log database for various applications.

Author: Soham Butala
Email: sbutala@uw.edu
Date: August 22, 2023
"""

from datetime import datetime
import os
import sqlite3
import threading
import json
from pathlib import Path

from pydantic import BaseModel
from echoflow.config.models.db_log_model import Log_Data, DB_Log

db_connections = threading.local()


def get_connection(path: str = '') -> sqlite3.Connection:
    """
    Gets a SQLite database connection.

    Args:
        path (str): The path to the database file.

    Returns:
        sqlite3.Connection: A connection to the SQLite database.

    Example:
        db_connection = get_connection('/path/to/db')
    """
    if not hasattr(db_connections, 'db_connection'):
        if os.path.isdir(path) == False:
            dir = Path(path)
            dir.mkdir(exist_ok=True, parents=True)
            os.chmod(dir, 0o777)
        db_connections.db_connection = sqlite3.connect(path+'/echoflow.db')
    return db_connections.db_connection


def execute_sql(conn, query):
    """
    Executes an SQL query on the provided connection.

    Args:
        conn (sqlite3.Connection): The database connection.
        query (str): The SQL query to execute.

    Returns:
        Any: The result of the query.

    Example:
        connection = get_connection('/path/to/db')
        query_result = execute_sql(connection, 'SELECT * FROM table_name')
    """
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    conn.commit()
    cursor.close()
    return result


def create_table(conn, table_name, columns):
    """
    Creates a table in the database.

    Args:
        conn (sqlite3.Connection): The database connection.
        table_name (str): The name of the table to create.
        columns (List[str]): List of column definitions.

    Example:
        connection = get_connection('/path/to/db')
        create_table(connection, 'table_name', ['column1 INT', 'column2 TEXT'])
    """
    query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"
    execute_sql(conn, query)


def create_log_table(conn):
    """
    Creates a 'log' table in the database.

    Args:
        conn (sqlite3.Connection): The database connection.

    Example:
        connection = get_connection('/path/to/db')
        create_log_table(connection)
    """
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS log (
        run_id INTEGER PRIMARY KEY AUTOINCREMENT,
        start_time TEXT,
        end_time TEXT,
        data TEXT,
        status TEXT,
        error TEXT
    );
    '''
    execute_sql(conn=conn, query=create_table_query)


def insert_log_data_by_conn(conn: sqlite3.Connection, log: DB_Log):
    """
    Inserts log data into the 'log' table using the provided connection.

    Args:
        conn (sqlite3.Connection): The database connection.
        log (DB_Log): The log data to insert.

    Returns:
        int: The ID of the inserted log.

    Example:
        connection = get_connection('/path/to/db')
        log_data = DB_Log(...)
        log_id = insert_log_data_by_conn(connection, log_data)
    """

    insert_query = '''
    INSERT INTO log (run_id, start_time, end_time, data, status, error)
    VALUES (?, ?, ?, ?, ?, ?);
    '''
    data_json = json.dumps(convert_to_serializable_dict(log.data))
    res = conn.execute(insert_query, (log.run_id, log.start_time,
                       log.end_time, data_json, log.status, log.error))
    id = res.lastrowid
    conn.commit()
    return id


def insert_log_data_by_path(path: str, log: DB_Log):
    """
    Inserts log data into the 'log' table using the database path.

    Args:
        path (str): The path to the database.
        log (DB_Log): The log data to insert.

    Returns:
        int: The ID of the inserted log.

    Example:
        log_data = DB_Log(...)
        log_id = insert_log_data_by_path('/path/to/db', log_data)
    """
    conn = get_connection(path=path)
    return insert_log_data_by_conn(conn, log)


def convert_to_serializable_dict(obj):
    """
    Converts objects to a serializable dictionary format.

    Args:
        obj (Any): The object to convert.

    Returns:
        Union[BaseModel, dict, List[dict], Any]: The serialized dictionary.

    Example:
        serialized_data = convert_to_serializable_dict(some_object)
    """
    if isinstance(obj, BaseModel):
        return obj.dict()
    elif isinstance(obj, dict):
        return {str(key): convert_to_serializable_dict(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_serializable_dict(item) for item in obj]
    else:
        return obj


def update_log_data_by_conn(conn: sqlite3.Connection, log: DB_Log):
    """
    Updates log data in the 'log' table using the provided connection.

    Args:
        conn (sqlite3.Connection): The database connection.
        log (DB_Log): The updated log data.

    Example:
        connection = get_connection('/path/to/db')
        updated_log_data = DB_Log(...)
        update_log_data_by_conn(connection, updated_log_data)
    """
    update_query = '''
    UPDATE log
    SET start_time = ?, end_time = ?, data = ?, status = ?, error =?
    WHERE run_id = ?;
    '''
    data_json = json.dumps(convert_to_serializable_dict(log.data))
    conn.execute(
        update_query,
        (log.start_time, log.end_time, data_json,
         log.status, log.error, log.run_id),
    )
    conn.commit()


def update_log_data_by_path(path: str, log: DB_Log):
    """
    Updates log data in the 'log' table using the database path.

    Args:
        path (str): The path to the database.
        log (DB_Log): The updated log data.

    Example:
        updated_log_data = DB_Log(...)
        update_log_data_by_path('/path/to/db', updated_log_data)
    """
    conn = get_connection(path=path)
    return update_log_data_by_conn(conn, log)


def parse_all_log_data(conn):
    """
    Parses and retrieves all log data from the 'log' table.

    Args:
        conn (sqlite3.Connection): The database connection.

    Returns:
        List[DB_Log]: List of parsed log objects.

    Example:
        connection = get_connection('/path/to/db')
        logs = parse_all_log_data(connection)
    """
    select_query = '''
    SELECT run_id, start_time, end_time, data, status, error
    FROM log;
    '''

    rows = execute_sql(conn, select_query)

    print(rows)

    logs = []
    for row in rows:
        log_obj = parse_log(row)
        logs.append(log_obj)

    return logs


def get_last_log(conn):
    """
    Retrieves the most recent log entry from the 'log' table.

    Args:
        conn (sqlite3.Connection): The database connection.

    Returns:
        DB_Log: The parsed log object.

    Example:
        connection = get_connection('/path/to/db')
        last_log_entry = get_last_log(connection)
    """
    select_query = '''
    SELECT run_id, start_time, end_time, data, status, error
    FROM log
    ORDER BY id DESC
    LIMIT 1;
    '''

    last_log = DB_Log()
    row = execute_sql(conn, select_query)

    if row:
        last_log = parse_log(row)

    return last_log


def parse_log(row):
    """
    Parses a row of log data and creates a DB_Log object.

    Args:
        row (Tuple): The row of log data.

    Returns:
        DB_Log: The parsed log object.

    Example:
        log_row = (1, '2023-08-15', '2023-08-16', {...}, 'success', None)
        log_obj = parse_log(log_row)
    """
    run_id, start_time, end_time, data_json, status, error = row
    data_dict = json.loads(data_json)

    log_data_dict = {}
    for key, value in data_dict.items():
        log_data_obj = Log_Data(**value)
        log_data_dict[key] = log_data_obj

    log_obj = DB_Log(run_id=run_id, start_time=start_time,
                     end_time=end_time, data=log_data_dict, status=status, error=error)

    return log_obj
