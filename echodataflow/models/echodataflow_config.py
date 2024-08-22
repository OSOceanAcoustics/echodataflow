"""
Module containing configuration classes related to Echodataflow and Prefect integration.

This module defines the following configuration classes:
    - EchodataflowPrefectConfig: Configuration for integrating Echodataflow with Prefect.
    - BaseConfig: Base configuration for Echodataflow.
    - EchodataflowConfig: Configuration for the Echodataflow library.

Classes:
    - EchodataflowPrefectConfig(Block): Configuration for integrating Echodataflow with Prefect.
    - BaseConfig(Block): Base configuration for Echodataflow.
    - EchodataflowConfig(Block): Configuration for the Echodataflow library.

Functions:
    No functions are defined in this module.

Author: Soham Butala
Email: sbutala@uw.edu
Date: August 22, 2023
"""
import json
from typing import Any, Dict, List, Optional

from prefect.blocks.core import Block
from pydantic import SecretStr

from .datastore import StorageType


class EchodataflowPrefectConfig(Block):
    """
    Configuration class for integrating Echodataflow with Prefect.

    Attributes:
        prefect_api_key (Optional[SecretStr]): Prefect API key.
        prefect_account_id (Optional[SecretStr]): Prefect account ID.
        prefect_workspace_id (Optional[SecretStr]): Prefect workspace ID.
        profile_name (str): Profile name for local Prefect setup.

    Methods:
        get_api_url(self): Get the API URL based on configuration.
    """

    class Config:
        arbitrary_types_allowed = True

    prefect_account_id: str = None
    prefect_api_key: str = None
    prefect_workspace_id: str = None
    profile_name: str = None

    def get_api_url(self):
        """
        Get the API URL based on configuration.

        Returns:
            str: The API URL.
        """
        if self.prefect_api_key is not None:
            return f"https://api.prefect.cloud/api/accounts/{ self.prefect_account_id }/workspaces/{ self.prefect_workspace_id }"
        elif self.profile_name == "echodataflow_prefect_local":
            return "127.0.0.1:4200"


class BaseConfig(Block):
    """
    Base configuration class for Echodataflow.

    Attributes:
        name (str): The name of the configuration.
        type (StorageType): The type of the configuration.
        active (Optional[bool]): Whether the configuration is active.
        options (Optional[Dict[str, Any]]): Additional configuration options.

    Methods:
        No methods are defined in this class.
    """

    name: str
    type: StorageType
    active: Optional[bool] = False
    options: Optional[Dict[str, Any]] = {}


class EchodataflowConfig(Block):
    """
    Configuration class for the Echodataflow library.

    Attributes:
        active (Optional[str]): The active configuration.
        prefect_configs (Optional[List[str]]): List of Prefect configuration names.
        blocks (Optional[List[BaseConfig]]): List of BaseConfig instances.

    Methods:
        No methods are defined in this class.
    """

    active: Optional[str] = None
    prefect_configs: Optional[List[str]]
    blocks: Optional[List[BaseConfig]] = []
