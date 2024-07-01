"""
Output Model for Echodataflow

This module defines a Pydantic model for representing output data in Echodataflow.

Classes:
    Output (BaseModel): Model for representing output data.

Author: Soham Butala
Email: sbutala@uw.edu
Date: August 22, 2023
"""
from typing import Any, Dict

from pydantic import BaseModel


class Output(BaseModel):
    """
    Model for representing output data.

    Attributes:
        data (Any): The output data.
        passing_params (Dict[str, Any]): A dictionary of passing parameters. Default is an empty dictionary.
    """
    

    data: Any = None
    passing_params: Dict[str, Any] = {}
