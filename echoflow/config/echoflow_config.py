"""
Module containing configuration classes related to Echoflow and Prefect integration.

This module defines the following configuration classes:
    - EchoflowPrefectConfig: Configuration for integrating Echoflow with Prefect.
    - BaseConfig: Base configuration for Echoflow.
    - EchoflowConfig: Configuration for the Echoflow library.

Classes:
    - EchoflowPrefectConfig(Block): Configuration for integrating Echoflow with Prefect.
    - BaseConfig(Block): Base configuration for Echoflow.
    - EchoflowConfig(Block): Configuration for the Echoflow library.

Functions:
    No functions are defined in this module.

Author: Soham Butala
Email: sbutala@uw.edu
Date: August 22, 2023
"""
from typing import Any, Dict, List, Optional
from prefect.blocks.core import Block
from pydantic import SecretStr

class EchoflowPrefectConfig(Block):
    """
    Configuration class for integrating Echoflow with Prefect.

    Attributes:
        prefect_api_key (Optional[SecretStr]): Prefect API key.
        prefect_account_id (Optional[SecretStr]): Prefect account ID.
        prefect_workspace_id (Optional[SecretStr]): Prefect workspace ID.
        profile_name (str): Profile name for local Prefect setup.

    Methods:
        get_api_url(self): Get the API URL based on configuration.
    """
    prefect_api_key: Optional[SecretStr] = None
    prefect_account_id: Optional[SecretStr] = None
    prefect_workspace_id: Optional[SecretStr] = None
    profile_name: str = None

    def get_api_url(self):
        """
        Get the API URL based on configuration.

        Returns:
            str: The API URL.
        """
        if self.prefect_api_key is not None:
            return f"https://api.prefect.cloud/api/accounts/{ self.prefect_account_id.get_secret_value() }/workspaces/{ self.prefect_workspace_id.get_secret_value() }" 
        elif self.profile_name == "echoflow_prefect_local":
            return "127.0.0.1:4200"

class BaseConfig(Block):
    """
    Base configuration class for Echoflow.

    Attributes:
        name (str): The name of the configuration.
        type (str): The type of the configuration.
        active (Optional[bool]): Whether the configuration is active.
        options (Optional[Dict[str, Any]]): Additional configuration options.

    Methods:
        No methods are defined in this class.
    """
    name: str
    type: str
    active: Optional[bool] = False
    options: Optional[Dict[str, Any]] = {}

class EchoflowConfig(Block):
    """
    Configuration class for the Echoflow library.

    Attributes:
        active (Optional[str]): The active configuration.
        prefect_configs (Optional[List[str]]): List of Prefect configuration names.
        blocks (Optional[List[BaseConfig]]): List of BaseConfig instances.

    Methods:
        No methods are defined in this class.
    """
    active: Optional[str] = None
    prefect_configs : Optional[List[str]]
    blocks: Optional[List[BaseConfig]] = []