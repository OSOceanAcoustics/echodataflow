"""
Echoflow Utility Functions

This module provides utility functions for configuring Prefect profiles, checking internet connectivity,
and managing Echoflow configurations.

Classes:
    None

Functions:
    check_internet_connection(host="8.8.8.8", port=53, timeout=5)
    echoflow_create_prefect_profile(name: str, api_key: str = None, workspace_id: str = None, account_id: str = None, set_active: bool = True)
    load_profile(name: str)
    get_active_profile()
    echoflow_start(dataset_config: Union[Dict[str, Any], str], pipeline_config: Union[Dict[str, Any], str], logging_config: Union[Dict[str, Any], str] = {}, storage_options: Union[Dict[str, Any], Block] = None, options: Optional[Dict[str, Any]] = {})
    update_prefect_config(prefect_api_key: Optional[str] = None, prefect_account_id: Optional[str] = None, prefect_workspace_id: Optional[str] = None, profile_name: str = None, active: bool = True)
    update_base_config(name: str, b_type: str, active: bool = False, options: Dict[str, Any] = {})
    echoflow_config_AWS(aws_key: str, aws_secret: str, token: str = None, name: str = "echoflow-aws-credentials", region: str = None, options: Dict[str, Any] = {}, active: bool = False)
    echoflow_config_AZ_cosmos(name: str = "echoflow-az-credentials", connection_string: str = None, options: Dict[str, Any] = {}, active: bool = False)
    load_credential_configuration(): Loads credentials from an `.ini` configuration file and creates corresponding credential blocks.

Author: Soham Butala
Email: sbutala@uw.edu
Date: August 22, 2023
"""

from enum import Enum
import json
import toml
import asyncio
import os
import configparser
from typing import Any, Dict, List, Optional, Union
from echoflow.config.models.echoflow_config import BaseConfig, EchoflowConfig, EchoflowPrefectConfig
from echoflow.config.models.datastore import StorageType

from echoflow.stages.subflows.pipeline_trigger import pipeline_trigger

from prefect.blocks.core import Block
from prefect_aws import AwsCredentials
from prefect_azure import AzureCosmosDbCredentials
import socket
from pydantic import SecretStr

from echoflow.stages.utils.config_utils import get_storage_options, load_block


def check_internet_connection(host="8.8.8.8", port=53, timeout=5):
    """
    Check if there is an active internet connection.

    Args:
        host (str, optional): IP address or hostname to check the connection to. Defaults to "8.8.8.8".
        port (int, optional): Port number to check the connection on. Defaults to 53 (DNS port).
        timeout (int, optional): Timeout for the connection check in seconds. Defaults to 5.

    Returns:
        bool: True if the connection is successful, False otherwise.

    Example:
        # Check if internet connection is available
        internet_available = check_internet_connection()
        print("Internet connection available:", internet_available)
    """
    try:
        # Set a timeout for the connection check
        socket.setdefaulttimeout(timeout)
        # Create a socket and connect to the host and port
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((host, port))
        return True
    except Exception as e:
        return False


def echoflow_create_prefect_profile(
    name: str,
    api_key: str = None,
    workspace_id: str = None,
    account_id: str = None,
    set_active: bool = True,
):
    """
    Create or update a Prefect profile in the local configuration file.

    Args:
        name (str): Name of the profile.
        api_key (str, optional): Prefect API key. Defaults to None.
        workspace_id (str, optional): Prefect workspace ID. Defaults to None.
        account_id (str, optional): Prefect account ID. Defaults to None.
        set_active (bool, optional): Set the profile as active. Defaults to True.

    Example:
        # Create a new Prefect profile
        echoflow_create_prefect_profile(
            name="my-profile",
            api_key="my-api-key",
            workspace_id="my-workspace-id",
            account_id="my-account-id",
            set_active=True,
        )
    """
    config_path = os.path.expanduser("~/.prefect/profiles.toml")
    with open(config_path, "r") as f:
        config = toml.load(f)

    # Update the active profile if specified
    if set_active:
        config["active"] = name

    profiles = config["profiles"]
    if api_key is not None and workspace_id is not None and account_id is not None:
        profiles[name] = {
            "PREFECT_API_KEY": api_key,
            "PREFECT_API_URL": f"https://api.prefect.cloud/api/accounts/{ account_id }/workspaces/{ workspace_id }",
        }
    else:
        profiles[name] = {}

    # Save the updated configuration file
    with open(config_path, "w") as f:
        toml.dump(config, f)

    # Does not update if switching from cloud to local or vice-versa, but updates the old profile which is active. This is the default behaviour of Prefect.
    update_prefect_config(
        prefect_api_key=api_key,
        prefect_workspace_id=workspace_id,
        prefect_account_id=account_id,
        profile_name=name,
    )


def load_profile(name: str):
    """
    Load an existing Prefect profile by setting it as the active profile.

    Args:
        name (str): Name of the profile to load.

    Example:
        # Load an existing Prefect profile
        load_profile("my-profile")
    """

    # Load the existing Prefect configuration file
    config_path = os.path.expanduser("~/.prefect/profiles.toml")
    with open(config_path, "r") as f:
        config = toml.load(f)

    # Check if the specified profile exists
    if config.get("profiles").get(name) is None:
        raise ValueError("No such profile exists. Please try creating profile with this name")
    
    # Set the specified profile as the active profile
    config["active"] = name

    # Save the updated configuration file
    with open(config_path, "w") as f:
        toml.dump(config, f)


def get_active_profile():
    """
    Get the configuration of the active Prefect profile.

    Returns:
        dict: Configuration of the active profile.

    Example:
        # Get the configuration of the active Prefect profile
        active_profile = get_active_profile()
        print("Active profile configuration:", active_profile)
    """
    # Load the existing Prefect configuration file
    config_path = os.path.expanduser("~/.prefect/profiles.toml")
    with open(config_path, "r") as f:
        config = toml.load(f)

    profiles = config["profiles"]

    # Find the active profile and return its configuration
    for p in profiles.keys():
        if p == config["active"]:
            return profiles[p]

    raise ValueError("No profile found.")


def echoflow_start(
    dataset_config: Union[Dict[str, Any], str],
    pipeline_config: Union[Dict[str, Any], str],
    logging_config: Union[Dict[str, Any], str] = {},
    storage_options: Union[Dict[str, Any], Block] = None,
    options: Optional[Dict[str, Any]] = {}
):
    
    """
    Start an Echoflow pipeline execution.

    Args:
        dataset_config (Union[Dict[str, Any], str]): Configuration for the dataset to be processed.
        pipeline_config (Union[Dict[str, Any], str]): Configuration for the pipeline to be executed.
        logging_config (Union[Dict[str, Any], str], optional): Configuration for logging. Defaults to {}.
        storage_options (Union[Dict[str, Any], Block], optional): Storage options for accessing data. Defaults to None.
        options (Optional[Dict[str, Any]], optional): Additional options. Defaults to {}.

    Returns:
        Any: Result of the pipeline execution.

    Example:
        # Define configuration files and options
        dataset_config = "dataset_config.yaml"
        pipeline_config = "pipeline_config.yaml"
        logging_config = {}
        storage_options = {"key": "value"}
        options = {"storage_options_override": True}

        # Start the Echoflow pipeline
        result = echoflow_start(
            dataset_config=dataset_config,
            pipeline_config=pipeline_config,
            logging_config=logging_config,
            storage_options=storage_options,
            options=options
        )
        print("Pipeline execution result:", result)
    """

    if storage_options is not None:
        # Check if storage_options is a Block (fsspec storage) and convert it to a dictionary
        if isinstance(storage_options, Block):
            storage_options = get_storage_options(storage_options=storage_options)
    else:
        storage_options = {}

    # Try loading the Prefect config block
    try:
        echoflow_config = EchoflowConfig.load("echoflow-config", validate=False)
    except ValueError as e:
        print("No Prefect Cloud Configuration found. Creating Prefect Local named 'echoflow-local'. Please add your prefect cloud ")
        # Add local profile to echoflow config but keep default as active since user might configure using Prefect setup
        echoflow_create_prefect_profile(name="echoflow-local", set_active=False)

    # Check if program can connect to the Internet.
    if check_internet_connection() == False:
        active_profile = get_active_profile()
        if active_profile["PREFECT_API_KEY"] is not None:
            raise ValueError(
                "Please connect to internet or consider switching to a local prefect environment. This can be done by calling load_profile(name_of_local_prefect_profile or 'echoflow-local' if no prefect profile was created) method."
            )
        else:
            print("Using a local prefect environment. To go back to your cloud workspace call load_profile(<name>) with <name> of your cloud profile.")
    
    if options['storage_options_override'] is not None and options['storage_options_override'] == False:
        storage_options = {}

    # Call the actual pipeline
    return pipeline_trigger(
        dataset_config=dataset_config,
        pipeline_config=pipeline_config,
        logging_config=logging_config,
        storage_options=storage_options
    )


def update_prefect_config(
    prefect_api_key: Optional[str] = None,
    prefect_account_id: Optional[str] = None,
    prefect_workspace_id: Optional[str] = None,
    profile_name: str = None,
    active: bool = True,
):
    """
    Update or create a Prefect configuration in the echoflow configuration block.

    Args:
        prefect_api_key (Optional[str], optional): Prefect API key. Defaults to None.
        prefect_account_id (Optional[str], optional): Prefect account ID. Defaults to None.
        prefect_workspace_id (Optional[str], optional): Prefect workspace ID. Defaults to None.
        profile_name (str, optional): Name of the profile. Defaults to None.
        active (bool, optional): Set the profile as active. Defaults to True.

    Returns:
        Any: Updated EchoflowConfig instance.

    Example:
        # Update or create a Prefect configuration with API key and workspace ID
        update_prefect_config(
            prefect_api_key="my-api-key",
            prefect_workspace_id="my-workspace-id",
            profile_name="my-profile",
            active=True
        )
    """
    profiles: List[str] = []
    prefect_config = EchoflowPrefectConfig(
        prefect_account_id=prefect_account_id,
        prefect_workspace_id=prefect_workspace_id,
        prefect_api_key=prefect_api_key,
        profile_name=profile_name,
    )

    uuid = asyncio.run(prefect_config.save(name=profile_name, overwrite=True))

    active_profile: str = None
    if active:
        active_profile = profile_name
    profiles.append(profile_name)

    try:
        current_config: EchoflowConfig = asyncio.run(EchoflowConfig.load("echoflow-config", validate=False))
        if current_config.prefect_configs is not None:

            if active_profile is None:            
                active_profile = current_config.active

            profiles = current_config.prefect_configs

            for p in profiles:
                if p == profile_name:
                    profiles.remove(p)
            profiles.append(profile_name)
        ecfg = asyncio.run(EchoflowConfig(active=active_profile, prefect_configs=profiles, blocks=current_config.blocks).save(
            "echoflow-config", overwrite=True
        ))
    except ValueError as e:
        ecfg = asyncio.run(EchoflowConfig(active=active_profile, prefect_configs=profiles, blocks=[]).save(
            "echoflow-config", overwrite=True
        ))
    return ecfg


def update_base_config(name: str, b_type: StorageType, active: bool = False, options: Dict[str, Any] = {}):
    """
    Update or create a base configuration in the echoflow configuration block.

    Args:
        name (str): Name of the configuration.
        b_type (StorageType): Type of the configuration.
        active (bool, optional): Set the configuration as active. Defaults to False.
        options (Dict[str, Any], optional): Additional options. Defaults to {}.

    Returns:
        Any: Updated EchoflowConfig instance.

    Example:
        # Update or create a base configuration
        update_base_config(
            name="my-aws-config",
            b_type="AWS",
            active=True,
            options={"option_key": "option_value"}
        )
    """
    aws_base = BaseConfig(name=name, type=b_type, active=active, options=options)
    ecfg: Any = None
    try:
        blocks: List[BaseConfig] = []
        current_config = asyncio.run(EchoflowConfig.load("echoflow-config", validate=False))

        if current_config.blocks is not None:
            blocks = current_config.blocks
            for b in blocks:
                if b.name == name:
                    blocks.remove(b)
        blocks.append(aws_base)
        ecfg = asyncio.run(EchoflowConfig(
            prefect_configs=current_config.prefect_configs, blocks=blocks
        ).save("echoflow-config", overwrite=True))
    except ValueError as e:
        ecfg = asyncio.run(EchoflowConfig(active=None, prefect_configs=[], blocks=[aws_base]).save(
            "echoflow-config", overwrite=True
        ))
    return ecfg


def echoflow_config_AWS(
    aws_access_key_id: str,
    aws_secret_access_key: str,
    aws_session_token: str = None,
    name: str = "echoflow-aws-credentials",
    region_name: str = None,
    options: Union[str, Dict[str, Any]] = {},
    active: bool = False,
    **kwargs
):
    """
    Configure AWS credentials in the echoflow configuration block.

    Args:
        aws_access_key_id (str): AWS access key.
        aws_secret_access_key (str): AWS secret access key.
        aws_session_token (str, optional): AWS session token. Defaults to None.
        name (str, optional): Name of the configuration. Defaults to "echoflow-aws-credentials".
        region_name (str, optional): AWS region name. Defaults to None.
        options (str, Dict[str, Any], optional): Additional options. Defaults to {}.
        active (bool, optional): Set the configuration as active. Defaults to False.

    Example:
        # Configure AWS credentials
        echoflow_config_AWS(
            aws_key="my-access-key",
            aws_secret="my-secret-key",
            token="my-session-token",
            region="us-west-1",
            name="my-aws-credentials",
            active=True,
            options={"option_key": "option_value"}
        )
    """
    coro = asyncio.run(AwsCredentials(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token,
        region_name=region_name,
    ).save(name, overwrite=True))
    if isinstance(options, str):
        options = json.loads(options)
    update_base_config(name=name, active=active, options=options, b_type=StorageType.AWS)

def echoflow_config_AZ_cosmos(
    name: str = "echoflow-az-credentials",
    connection_string: str = None,
    options: Union[str, Dict[str, Any]] = {},
    active: bool = False,
    **kwargs
):
    """
    Configure Azure Cosmos DB credentials in the local configuration file.

    Args:
        name (str, optional): Name of the configuration. Defaults to "echoflow-az-credentials".
        connection_string (str): Azure Cosmos DB connection string.
        options (str, Dict[str, Any], optional): Additional options. Defaults to {}.
        active (bool, optional): Set the configuration as active. Defaults to False.

    Raises:
        ValueError: If connection string is empty.

    Example:
        # Configure Azure Cosmos DB credentials
        echoflow_config_AZ_cosmos(
            name="my-az-cosmos-credentials",
            connection_string="my-connection-string",
            active=True,
            options={"option_key": "option_value"}
        )
    """
    if connection_string is None:
        raise ValueError("Connection string cannot be empty.")
    coro = asyncio.run(AzureCosmosDbCredentials(
        connection_string=connection_string
    ).save(name, overwrite=True))
    if isinstance(options, str):
        options = json.loads(options)
    update_base_config(name=name, active=active, options=options, b_type=StorageType.AZCosmos)

def load_credential_configuration(sync: bool = False):
    """
    Load credentials from an `.ini` configuration file and create corresponding credential blocks.

    This function reads the `credentials.ini` file from the `.echoflow` directory located in the user's home directory.
    It parses the contents of the `.ini` file and creates credential blocks using Echoflow's `echoflow_config_AWS`
    and `echoflow_config_AZ_cosmos` functions. Each section in the `.ini` file corresponds to a different type of
    credential block, such as AWS and Azure Cosmos DB.

    Args:
        sync (bool): If True, syncs blocks updated using Prefect UI. Defaults to False.

    Example:
        If `credentials.ini` contains the following:

        [AWS]
        aws_key = my-access-key
        aws_secret = my-secret-key
        region = us-west-1

        [AZCosmos]
        name = my-az-cosmos-credentials
        connection_string = my-connection-string

        Calling `load_credential_configuration()` will create an AWS credential block with the provided credentials
        and an Azure Cosmos DB credential block with the specified connection string.

    Note:
        - Ensure the `credentials.ini` file is correctly formatted with appropriate sections and keys.
        - Supported section names are "AWS" and "AZCosmos".
        - Any unrecognized sections will be reported.
        - If `sync` is True, the function will sync blocks with the Prefect UI.

    Raises:
        FileNotFoundError: If the `credentials.ini` file is not found.
        ValueError: If no Echoflow configuration is found when `sync` is True.
    """
    config = configparser.ConfigParser()
    home_directory = os.path.expanduser("~")

    # Create the directory if it doesn't exist
    config_directory = os.path.join(home_directory, ".echoflow")
    os.makedirs(config_directory, exist_ok=True)

    # Write the .ini file
    ini_file_path = os.path.join(config_directory, "credentials.ini")
    config.read(ini_file_path)

    if sync:
        current_config: EchoflowConfig = None
        try:
            current_config = asyncio.run(EchoflowConfig.load("echoflow-config", validate=False))
            if current_config is not None:

                for base in current_config.blocks:
       
                    block = load_block(base.name, base.type)
                    block_dict = dict(block)
                    block_dict['name'] = base.name
                    block_dict['active'] = base.active
                    block_dict['options'] = json.dumps(base.options)
                    block_dict['provider'] = base.type
                    converted_dict = {}
                    for key, value in block_dict.items():
                        if isinstance(value, SecretStr):
                            value = value.get_secret_value()
                        if isinstance(value, Enum):
                            value = value.value
                        converted_dict[key] = str(value)
                    config[base.name] = converted_dict
                with open(ini_file_path, "w") as config_file:
                    config.write(config_file) 
        except ValueError:
            raise("No Echoflow configuration found.")      
    for section in config.sections():
        provider = config.get(section,'provider')
        data_dict = dict(config[section])
        data_dict['name'] = section
        data_dict.pop('provider')
        if provider == "AWS":
            echoflow_config_AWS(**data_dict)
        elif provider == "AZCosmos":
            echoflow_config_AZ_cosmos(**data_dict)
        else:
            print(f"Unknown section: {provider}")

