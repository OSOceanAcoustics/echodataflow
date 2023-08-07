from datetime import datetime
import logging
from typing import Dict, Union
import psutil
import yaml
from echoflow.config.models.dataset import Dataset
from echoflow.config.models.db_log_model import DB_Log, Log_Data, Process
from echoflow.config.models.pipeline import Recipe
from echoflow.stages_v2.utils.databse_utils import (
    create_log_table,
    get_connection,
    get_last_log,
    update_log_data_by_conn,
    insert_log_data_by_conn
)


class Singleton_Echoflow:
    _instance: "Singleton_Echoflow" = None

    pipeline: Recipe
    dataset: Dataset
    db_log: DB_Log
    logger: logging.Logger = None
    log_level: int  = 0

    def __new__(
        cls,
        log_file: Union[Dict[str, str], str] = None,
        pipeline: Recipe = None,
        dataset: Dataset = None 
    ) -> "Singleton_Echoflow":
        
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            if log_file is not None:
                cls._instance.logger = cls._instance.logger_init(log_file)
                cls._instance.log_level = cls._instance.logger.level
                
        cls._instance.pipeline = pipeline
        cls._instance.dataset = dataset
        # cls._instance.db_log = cls._instance.setup_echoflow_db()
        return cls._instance

    @classmethod
    def get_instance(self) -> "Singleton_Echoflow":
        return self._instance
    
    @classmethod
    def get_logger(self) -> "logging.Logger":
        return self.logger

    def logger_init(self, log_file: Union[Dict[str, str], str]) -> None:
        if type(log_file) == str:
            with open(log_file, "r") as file:
                logging_config_dict = yaml.safe_load(file.read())
                logging.config.dictConfig(logging_config_dict)
        else:
            logging.config.dictConfig(log_file)
        return logging.getLogger("echoflow")

    def setup_echoflow_db(self):
        last_log = DB_Log()
        try:
            conn = get_connection(self.pipeline.database_path)
            create_log_table(conn=conn)

            if self.pipeline.use_previous_recipe == True:
                last_log = get_last_log(conn)

            last_log.start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            last_log.run_id = None

            return last_log
        except Exception as e:
            print("Failed to create Database with below error")
            print(e)

    def log(self, msg, level, extra):
        self.logger.log(level=logging.DEBUG, msg=msg, extra=extra)
        self.logger.log(level=logging.ERROR, msg=msg, extra=extra)
        self.logger.log(level=logging.INFO, msg=msg, extra=extra)
        self.logger.log(level=logging.WARNING, msg=msg, extra=extra)

    def add_new_process(self, process: Process, name: str):
        process.end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        if name not in self.db_log.data:
            self.db_log.data[name] = Log_Data(name=name)
        self.db_log.data[name].process_stack.append(process)

    def insert_log_data(self):
        self.db_log.end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        try:
            conn = get_connection(self._instance.pipeline.database_path)
            if self.db_log.run_id is None:
                return insert_log_data_by_conn(conn, log=self.db_log)
            else:
                update_log_data_by_conn(conn, log=self.db_log)
                return self.db_log.run_id
        except Exception as e:
            print(e)
        finally:
            conn.close()
        

    def log_memory_usage(self):
        process = psutil.Process()
        mem = process.memory_info().rss
        return mem
