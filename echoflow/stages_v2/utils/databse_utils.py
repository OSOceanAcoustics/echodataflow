from datetime import datetime
import os
import sqlite3
import threading
import json
from pathlib import Path

from pydantic import BaseModel
from echoflow.config.models.db_log_model import Log_Data, DB_Log

db_connections = threading.local()

# Function to get a database connection
def get_connection(path: str='') -> sqlite3.Connection:
    if not hasattr(db_connections, 'db_connection'):
        if os.path.isdir(path) == False:
            dir = Path(path)
            dir.mkdir(exist_ok=True, parents=True)
            os.chmod(dir, 0o777)
        db_connections.db_connection = sqlite3.connect(path+'/echoflow.db')
    return db_connections.db_connection


def execute_sql(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    conn.commit()
    cursor.close()
    return result

def create_table(conn, table_name, columns):
    query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"
    execute_sql(conn, query)


def create_log_table(conn):
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

def insert_log_data_by_conn(conn : sqlite3.Connection , log: DB_Log):
    insert_query = '''
    INSERT INTO log (run_id, start_time, end_time, data, status, error)
    VALUES (?, ?, ?, ?, ?, ?);
    '''
    data_json = json.dumps(convert_to_serializable_dict(log.data))
    res = conn.execute(insert_query, (log.run_id, log.start_time, log.end_time, data_json, log.status, log.error))
    id = res.lastrowid
    conn.commit()
    return id

def insert_log_data_by_path(path: str, log: DB_Log):
    conn = get_connection(path=path)
    return insert_log_data_by_conn(conn, log)

def convert_to_serializable_dict(obj):
    if isinstance(obj, BaseModel):
        return obj.dict()
    elif isinstance(obj, dict):
        return {str(key): convert_to_serializable_dict(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_serializable_dict(item) for item in obj]
    else:
        return obj


def update_log_data_by_conn(conn : sqlite3.Connection, log : DB_Log):
    update_query = '''
    UPDATE log
    SET start_time = ?, end_time = ?, data = ?, status = ?, error =?
    WHERE run_id = ?;
    '''
    data_json = json.dumps(convert_to_serializable_dict(log.data))
    conn.execute(
        update_query,
        (log.start_time, log.end_time, data_json, log.status, log.error, log.run_id),
    )
    conn.commit()

def update_log_data_by_path(path: str, log: DB_Log):
    conn = get_connection(path=path)
    return update_log_data_by_conn(conn, log)


def parse_all_log_data(conn):
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
    run_id, start_time, end_time, data_json, status, error = row
    data_dict = json.loads(data_json)

    log_data_dict = {}  
    for key, value in data_dict.items():
        log_data_obj = Log_Data(**value)  
        log_data_dict[key] = log_data_obj 
    
    log_obj = DB_Log(run_id=run_id, start_time=start_time, end_time=end_time, data=log_data_dict, status=status, error=error)

    return log_obj