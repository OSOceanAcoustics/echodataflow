"""
Logging Models for Echodataflow

This module defines Pydantic models for managing logging and process data in Echodataflow.

Classes:
    Process (BaseModel): Model for representing a process.
    Log_Data (BaseModel): Model for representing log data.
    DB_Log (BaseModel): Model for managing database log.

Author: Soham Butala
Email: sbutala@uw.edu
Date: August 22, 2023
"""

from datetime import datetime
from typing import Dict, List, Optional

from pydantic import BaseModel


class Process(BaseModel):
    """
    Model for representing a process.

    Attributes:
        name (str): The name of the process.
        start_time (str): The start time of the process. Default is the current datetime.
        end_time (Optional[str]): The end time of the process.
        status (bool): The status of the process. Default is False.
        error (Optional[str]): The error message associated with the process, if any.
    """

    name: str
    start_time: str = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    end_time: Optional[str]
    status: bool = False
    error: Optional[str]


class Log_Data(BaseModel):
    """
    Model for representing log data.

    Attributes:
        name (str): The name of the log data.
        process_stack (Optional[List[Process]]): A list of associated processes.
    """

    name: str
    process_stack: Optional[List[Process]] = []


class DB_Log(BaseModel):
    """
    Model for managing database log.

    Attributes:
        run_id (Optional[int]): The run ID of the log.
        start_time (Optional[str]): The start time of the log. Default is the current datetime.
        end_time (Optional[str]): The end time of the log.
        data (Optional[Dict[str, Log_Data]]): A dictionary of log data.
        status (str): The status of the log. Default is True.
        error (Optional[str]): The error message associated with the log, if any.
    """

    run_id: Optional[int]
    start_time: Optional[str] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    end_time: Optional[str]
    data: Optional[Dict[str, Log_Data]] = {}
    status: str = True
    error: Optional[str]
