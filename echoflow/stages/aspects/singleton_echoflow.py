"""
Module containing the Singleton_Echoflow class and utility methods.

This module defines the `Singleton_Echoflow` class, which implements a Singleton
pattern for managing echoflow-related configurations, logging, and database operations.
It also provides utility methods for logging, managing processes, inserting log data,
and logging memory usage.

Classes:
    Singleton_Echoflow: Singleton class for managing echoflow configurations and operations.

Functions:
    - get_instance(self) -> "Singleton_Echoflow": Get the instance of Singleton_Echoflow.
    - get_logger(self) -> "logging.Logger": Get the logger instance.
    - logger_init(self, log_file: Union[Dict[str, str], str]) -> None:
        Initialize the logger based on the provided log configuration.
    - setup_echoflow_db(self): Setup the echoflow database and return a DB_Log instance.
    - log(self, msg, level, extra): Log a message at the specified log levels.
    - add_new_process(self, process: Process, name: str): Add a new process to the DB_Log instance.
    - insert_log_data(self): Insert or update log data in the database.
    - log_memory_usage(self): Get the memory usage of the current process.
    - get_possible_next_functions(self, function_name): Retrieve a list of functions that can potentially be executed next
    - load(self): Load predefined rules from a file and establish dependency relationships

Author: Soham Butala
Email: sbutala@uw.edu
Date: August 22, 2023
"""
from datetime import datetime
import logging
from pathlib import Path
from typing import Dict, Union
import psutil
import yaml
from echoflow.config.models.datastore import Dataset
from echoflow.config.models.db_log_model import DB_Log, Log_Data, Process
from echoflow.config.models.pipeline import Recipe
from echoflow.config.models.rule_engine.dependency_engine import DependencyEngine
from echoflow.stages.utils.databse_utils import (
    create_log_table,
    get_connection,
    get_last_log,
    update_log_data_by_conn,
    insert_log_data_by_conn
)


class Singleton_Echoflow:
    _instance: "Singleton_Echoflow" = None

    rengine: DependencyEngine = DependencyEngine()
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
        cls._instance.rengine = cls._instance.load()
        return cls._instance

    @classmethod
    def get_instance(self) -> "Singleton_Echoflow":
        """
        Get the instance of the Singleton_Echoflow class.

        Returns:
            Singleton_Echoflow: The Singleton_Echoflow instance.
        """
        return self._instance
    
    @classmethod
    def get_logger(self) -> "logging.Logger":
        """
        Get the logger instance.

        Returns:
            logging.Logger: The logger instance.
        """
        return self.logger

    def logger_init(self, log_file: Union[Dict[str, str], str]) -> None:
        """
        Initialize the logger based on the provided log configuration.

        Args:
            log_file (Union[Dict[str, str], str]): The log configuration file or dictionary.
        """
        if type(log_file) == str:
            with open(log_file, "r") as file:
                logging_config_dict = yaml.safe_load(file.read())
                logging.config.dictConfig(logging_config_dict)
        else:
            logging.config.dictConfig(log_file)
        return logging.getLogger("echoflow")

    def setup_echoflow_db(self):
        """
        Setup the echoflow database and return a DB_Log instance.

        Returns:
            DB_Log: The initialized DB_Log instance.
        """
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
        """
        Log a message at the specified log levels.

        Args:
            msg: The message to be logged.
            level: The log level.
            extra: Extra information to include in the log record.
        """
        self.logger.log(level=logging.DEBUG, msg=msg, extra=extra)
        self.logger.log(level=logging.ERROR, msg=msg, extra=extra)
        self.logger.log(level=logging.INFO, msg=msg, extra=extra)
        self.logger.log(level=logging.WARNING, msg=msg, extra=extra)

    def add_new_process(self, process: Process, name: str):
        """
        Add a new process to the DB_Log instance.

        Args:
            process (Process): The process to be added.
            name (str): The name of the process.
        """
        process.end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        if name not in self.db_log.data:
            self.db_log.data[name] = Log_Data(name=name)
        self.db_log.data[name].process_stack.append(process)

    def insert_log_data(self):
        """
        Insert or update log data in the database.

        Returns:
            int: The inserted or updated log entry ID.
        """
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
        """
        Get the memory usage of the current process.

        Returns:
            int: The memory usage in bytes.
        """
        process = psutil.Process()
        mem = process.memory_info().rss
        return mem
    
    def get_possible_next_functions(self, function_name: str):
        """
        Get Possible Next Functions

        Retrieve a list of functions that can potentially be executed next, according
        to the defined rules and dependencies.

        Args:
            function_name (str): The name of the function that has completed.

        Returns:
            list: A list of functions that can be executed next.

        Example:
            # Get possible next functions after executing "data_download"
            next_functions = executor.get_possible_next_functions(function_name="data_download")
        """
        return self.rengine.get_possible_next_functions(function_name)
        
    def load(self):
        """
        Load Rules and Dependencies

        Load predefined rules from a file and establish dependency relationships
        between functions using the DependencyEngine.

        Returns:
            DependencyEngine: The initialized DependencyEngine with loaded rules.

        Example:
            # Load rules from file and establish dependencies
            loaded_rengine = executor.load()
        """
        rengine: DependencyEngine = DependencyEngine()
        path = Path('../echoflow/config/models/rule_engine/echoflow_rules.txt')
        with open(path, 'r') as file:
            for line in file:
                target, dependent = line.strip().split(':')
                rengine.add_dependency(target_function=target, dependent_function=dependent)
        return rengine
