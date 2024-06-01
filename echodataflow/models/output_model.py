"""
Output Model for Echodataflow

This module defines a Pydantic model for representing output data in Echodataflow.

Classes:
    Output (BaseModel): Model for representing output data.

Author: Soham Butala
Email: sbutala@uw.edu
Date: August 22, 2023
"""
from typing import Any, Dict, List, Optional

from pydantic import BaseModel

class ErrorObject(BaseModel):
    errorFlag: bool = False
    error_desc: str = None

class EchodataflowObject(BaseModel):
    file_path: str = None
    filename: str = None
    month: Optional[str] = None
    year: Optional[str] = None
    jday: Optional[str] = None
    datetime: Optional[str] = None
    stages: Optional[Dict[str, Any]] = {}
    out_path: str = None
    group_name: str = 'DefaultGroup'
    error: ErrorObject = ErrorObject()
    local_path: str = None
        
class Group(BaseModel):
    
    group_name: str = "DefaultGroup"
    instrument: str = None
    data: List[EchodataflowObject] = []
    

class Output(BaseModel):
    """
    Model for representing output data.

    Attributes:
        data (Any): The output data.
        passing_params (Dict[str, Any]): A dictionary of passing parameters. Default is an empty dictionary.
    """
    

    group: Dict[str, Group] = {}
    passing_params: Dict[str, Any] = {}
