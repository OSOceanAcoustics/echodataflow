"""
Output Model for Echodataflow

This module defines a Pydantic model for representing input and output data of flows in Echodataflow.

Classes:
    ErrorObject (BaseModel): Model for representing an error object.
    EchodataflowObject (BaseModel): Model for representing an echodataflow object.
    Group (BaseModel): Model for representing a group.
    Output (BaseModel): Model for representing output data.

Author: Soham Butala
Email: sbutala@uw.edu
Date: June 5, 2024
"""
from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class ErrorObject(BaseModel):
    """
    Model for representing an error object.

    Attributes:
        errorFlag (bool): Flag indicating if there is an error. Default is False.
        error_desc (Optional[str]): Description of the error.
    """

    error_type: str = "EXTERNAL"
    errorFlag: bool = False
    error_desc: str = None


class EchodataflowObject(BaseModel):
    """
    Model for representing an echodataflow object.

    Attributes:
        file_path (Optional[str]): The file path.
        filename (Optional[str]): The filename.
        month (Optional[str]): The month.
        year (Optional[str]): The year.
        jday (Optional[str]): The Julian day.
        datetime (Optional[str]): The datetime.
        stages (Optional[Dict[str, Any]]): The stages of processing.
        out_path (Optional[str]): The output path.
        group_name (str): The group name. Default is 'DefaultGroup'.
        error (ErrorObject): The error object.
        local_path (Optional[str]): The local path.
    """

    file_path: str = None
    filename: str = None
    month: Optional[str] = None
    year: Optional[str] = None
    jday: Optional[str] = None
    datetime: Optional[str] = None
    stages: Optional[Dict[str, Any]] = {}
    out_path: str = None
    group_name: str = "DefaultGroup"
    error: ErrorObject = ErrorObject()
    local_path: str = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None


class Group(BaseModel):
    """
    Model for representing a group.

    Attributes:
        group_name (str): The name of the group. Default is 'DefaultGroup'.
        instrument (Optional[str]): The instrument associated with the group.
        data (List[EchodataflowObject]): List of echodataflow objects in the group.
    """

    group_name: str = "DefaultGroup"
    instrument: str = None
    data: List[EchodataflowObject] = []


class Output(BaseModel):
    """
    Model for representing output data.

    Attributes:
        group (Dict[str, Group]): Dictionary of groups.
        passing_params (Dict[str, Any]): A dictionary of passing parameters. Default is an empty dictionary.
    """

    group: Dict[str, Group] = {}
    passing_params: Dict[str, Any] = {}
