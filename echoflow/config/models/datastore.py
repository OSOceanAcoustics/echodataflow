"""
Echoflow Configuration Models

This module defines Pydantic models that represent different aspects of the Echoflow configuration.

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
from typing import Any, Dict, Optional
from pydantic import BaseModel
import jinja2


class StorageType(Enum):
    """
    Enumeration for storage types.

    Attributes:
        AWS: Amazon Web Services storage type.
        AZ: Azure storage type.
        GCP: Google Cloud Platform storage type.
    """
    AWS = "AWS"
    AZCosmos = "AZCosmos"
    GCP = "GCP"


class StorageOptions(BaseModel):
    """
    Model for defining storage options.

    Attributes:
        type (Optional[StorageType]): The type of storage.
        block_name (Optional[str]): The name of the block.
        anon (Optional[bool]): Whether to use anonymous access. Default is False.
    """
    type: Optional[StorageType]
    block_name: Optional[str]
    anon: Optional[bool] = False


class Parameters(BaseModel):
    """
    Model for defining parameters.

    Attributes:
        ship_name (str): The name of the ship.
        survey_name (str): The name of the survey.
        sonar_model (str): The model of the sonar.
    """
    ship_name: str
    survey_name: str
    sonar_model: str


class Transect(BaseModel):
    """
    Model for defining transect options.

    Attributes:
        file (Optional[str]): The file associated with the transect.
        storage_options (Optional[StorageOptions]): Storage options for the transect.
        storage_options_dict (Optional[Dict[str, Any]]): Additional storage options as a dictionary.
        default_transect_num (int): Default transect number.
    """
    file: Optional[str]
    storage_options: Optional[StorageOptions] = None
    storage_options_dict: Optional[Dict[str, Any]] = {}  
    default_transect_num: int = None


class Args(BaseModel):
    """
    Model for defining input arguments.

    Attributes:
        urlpath (str): The URL path.
        parameters (Parameters): Parameters for rendering.
        storage_options (Optional[StorageOptions]): Storage options for the arguments.
        storage_options_dict (Optional[Dict[str, Any]]): Additional storage options as a dictionary.
        transect (Optional[Transect]): Transect options.
        zarr_store (Optional[str]): Zarr store information.
        json_export (Optional[bool]): Whether to export in JSON format. Default is False.
        raw_json_path (Optional[str]): Path for raw JSON data.
    """
    urlpath: str
    parameters: Parameters
    storage_options: Optional[StorageOptions] = None
    storage_options_dict: Optional[Dict[str, Any]] = {}
    transect: Optional[Transect]
    zarr_store: Optional[str]
    json_export: Optional[bool] = False
    raw_json_path: Optional[str] = None

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


class Output(BaseModel):
    """
    Model for defining output options.

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
    passing_params: Optional[Dict[str, Any]]
