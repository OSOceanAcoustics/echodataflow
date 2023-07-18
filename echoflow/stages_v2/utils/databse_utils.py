import sqlite3
import threading
import json

from pydantic import BaseModel
from echoflow.config.models.log_object import Log, Log_Data

db_connections = threading.local()

# Function to get a database connection
def get_connection(path=''):
    if not hasattr(db_connections, 'db_connection'):
        # Create a new database connection if one doesn't exist for the current thread
        db_connections.db_connection = sqlite3.connect(path+'echoflow.db')
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
    conn.commit()


def create_log_table(conn):
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS log (
        run_id INTEGER PRIMARY KEY AUTOINCREMENT,
        start_time TEXT,
        end_time TEXT,
        data TEXT,
        status TEXT
    );
    '''
    execute_sql(conn=conn, query=create_table_query)
    conn.commit()


def insert_log_data(conn, log: Log):
    insert_query = '''
    INSERT INTO log (run_id, start_time, end_time, data, status)
    VALUES (?, ?, ?, ?, ?);
    '''
    data_json = json.dumps(log.data)
    conn.execute(insert_query, (log.run_id, log.start_time, log.end_time, data_json, log.status))
    conn.commit()

def convert_to_serializable_dict(obj):
    if isinstance(obj, BaseModel):
        return obj.dict()
    elif isinstance(obj, dict):
        return {str(key): convert_to_serializable_dict(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_serializable_dict(item) for item in obj]
    else:
        return obj

def insert_log_data(path: str, log: Log):
    conn = get_connection(path=path)
    insert_query = '''
    INSERT INTO log (run_id, start_time, end_time, data, status)
    VALUES (?, ?, ?, ?, ?);
    '''
    data_json = json.dumps(convert_to_serializable_dict(log.data))
    conn.execute(insert_query, (log.run_id, log.start_time, log.end_time, data_json, log.status))
    conn.commit()


def parse_all_log_data(conn):
    select_query = '''
    SELECT run_id, start_time, end_time, data, status
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
    SELECT run_id, start_time, end_time, status
    FROM log
    ORDER BY id DESC
    LIMIT 1;
    '''

    last_log = Log()
    row = execute_sql(conn, select_query)

    if row:
        last_log = parse_log(row)

    return last_log

def parse_log(row):
    run_id, start_time, end_time, data_json, status = row
    data_dict = json.loads(data_json)

    log_data_dict = {}  
    for key, value in data_dict.items():
        log_data_obj = Log_Data(**value)  
        log_data_dict[key] = log_data_obj 
    
    log_obj = Log(run_id=run_id, start_time=start_time, end_time=end_time, data=log_data_dict, status=status)

    return log_obj