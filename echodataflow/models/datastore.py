"""
Echodataflow Configuration Models

This module defines Pydantic models that represent different aspects of the Echodataflow configuration.

Classes:
    StorageType (Enum): An enumeration representing different storage types.
    StorageOptions (BaseModel): Model for defining storage options.
    Parameters (BaseModel): Model for defining parameters.
    Transect (BaseModel): Model for defining transect options.
    Args (BaseModel): Model for defining input arguments.
    Output (BaseModel): Model for defining output options.
    Dataset (BaseModel): Model for defining dataset configuration.

Author: Soham Butala
Email: sbutala@uw.edu
Date: August 22, 2023
"""

from enum import Enum
import re
from typing import Any, Dict, List, Optional, Union

import jinja2
from pydantic import BaseModel, field_validator, validator


class StorageType(Enum):
    """
    Enumeration for storage types.

    Attributes:
        AWS: Amazon Web Services storage type.
        AZCosmos: Azure storage type.
        GCP: Google Cloud Platform storage type.
    """

    AWS = "AWS"
    AZCosmos = "AZCosmos"
    GCP = "GCP"
    ECHODATAFLOW = "ECHODATAFLOW"
    EDFRUN = "EDFRUN"

class StorageOptions(BaseModel):
    """
    Model for defining storage options.

    Attributes:
        type (Optional[StorageType]): The type of storage.
        block_name (Optional[str]): The name of the block.
        anon (Optional[bool]): Whether to use anonymous access. Default is False.
    """

    type: Optional[StorageType] = None
    block_name: Optional[str] = None
    anon: Optional[bool] = False


class Parameters(BaseModel):
    """
    Model for defining parameters.

    Attributes:
        ship_name (Optional[str]): The name of the ship.
        survey_name (Optional[str]): The name of the survey.
        sonar_model (Optional[str]): The model of the sonar.
        file_name (Optional[str]): The name of the file.
    """

    ship_name: Optional[str] = None
    survey_name: Optional[str] = None
    sonar_model: Optional[str] = None
    file_name: Optional[str] = None


class Transect(BaseModel):
    """
    Model for defining transect options.

    Attributes:
        file (Optional[str]): The file associated with the transect.
        storage_options (Optional[StorageOptions]): Storage options for the transect.
        storage_options_dict (Optional[Dict[str, Any]]): Additional storage options as a dictionary.
        grouping_regex: Regex to parse group name from the file name.
    """

    file: Optional[str] = None
    storage_options: Optional[StorageOptions] = None
    storage_options_dict: Optional[Dict[str, Any]] = {}
    grouping_regex: Optional[str] = None


class Args(BaseModel):
    """
    Model for defining input arguments.

    Attributes:
        urlpath (str): The URL path.
        parameters (Parameters): Parameters for rendering.
        storage_options (Optional[StorageOptions]): Storage options for the arguments.
        storage_options_dict (Optional[Dict[str, Any]]): Additional storage options as a dictionary.
        group (Optional[Transect]): Transect options.
        group_name : Default transect name.
        zarr_store (Optional[str]): Zarr store information.
        json_export (Optional[bool]): Whether to export in JSON format. Default is False.
        raw_json_path (Optional[str]): Path for raw JSON data.
    """

    storepath: Optional[str] = None
    urlpath: Optional[str] = None
    storefolder: Optional[Union[List[str], str]] = None
    
    parameters: Optional[Parameters] = None
    storage_options: Optional[StorageOptions] = None
    storage_options_dict: Optional[Dict[str, Any]] = {}
    group: Optional[Transect] = None
    group_name: Optional[str] = None
    zarr_store: Optional[str] = None
    json_export: Optional[bool] = False
    raw_json_path: Optional[str] = None

    window_size: Optional[int] = 1
    time_travel_hours: Optional[int] = 0
    time_travel_mins: Optional[int] = 0
    rolling_size: Optional[int] = 1
    
    window_hours: Optional[int] = 0
    window_mins: Optional[int] = 30
    number_of_windows: Optional[int] = 1

    @property
    def rendered_path(self):
        """
        Rendered URL path from input parameters.

        Returns:
            str: Rendered URL path.
        """
        if self.parameters is not None:
            env = jinja2.Environment()
            template = env.from_string(self.urlpath)
            return template.render(self.parameters)
        return self.urlpath

    @property
    def store_path(self):
        """
        Rendered URL path from input parameters.

        Returns:
            str: Rendered URL path.
        """
        if self.parameters is not None:
            env = jinja2.Environment()
            template = env.from_string(self.storepath)
            return template.render(self.parameters)
        return self.storepath

    @property
    def store_folder(self):
        """
        Rendered URL path from input parameters.

        Returns:
            str: Rendered URL path.
        """
        if self.parameters is not None:
            env = jinja2.Environment()
            template = env.from_string(self.storefolder)
            return template.render(self.parameters)
        return self.storefolder
    
    @field_validator("group_name", mode="before", check_fields=True)
    def disallow_regex_chars(cls, v):
        if isinstance(v, int):
            v = str(v)
        regex_special_chars = r"[{}()|\[\]\\]"
        if re.search(regex_special_chars, v):
            raise ValueError("Field must not contain regex special characters")
        return v


class Output(BaseModel):
    """
    Model for defining pipeline destination options.

    Attributes:
        urlpath (str): The URL path for output.
        retention (bool): Retains output at each stage. Works as central flag for all the processes but does not trump the `save_offline` flag.
        overwrite (Optional[bool]): Whether to overwrite existing data. Default is True.
        storage_options (Optional[StorageOptions]): Storage options for the output.
        storage_options_dict (Optional[Dict[str, Any]]): Additional storage options as a dictionary.
    """

    urlpath: str
    retention: bool = True
    overwrite: Optional[bool] = True
    storage_options: Optional[StorageOptions] = None
    storage_options_dict: Optional[Dict[str, Any]] = {}


class Kafka(BaseModel):
    topic: str
    servers: List[str]


class EchodataflowLogs(BaseModel):
    kafka: Kafka = None


class Dataset(BaseModel):
    """
    Model for defining dataset configuration.

    Attributes:
        name (str): The name of the dataset.
        sonar_model (str): The model of the sonar.
        raw_regex (str): Regular expression for matching raw files.
        args (Args): Input arguments for the dataset.
        output (Output): Output options for the dataset.
        passing_params (Optional[Dict[str, Any]]): Additional passing parameters.
    """

    name: str
    sonar_model: str
    raw_regex: str
    args: Args
    output: Output
    passing_params: Optional[Dict[str, Any]] = None
    logging: EchodataflowLogs = None
