from typing import Any, Dict, Union

from prefect import task, get_run_logger

import yaml
from ..subflows.utils import extract_fs
from ..settings.models import MainConfig


def check_and_extract_config(
    config: Union[Dict[str, Any], str], storage_options: Dict[str, Any] = {}
) -> Dict[str, Any]:
    """
    Read config file and extract to a dictionary

    Parameters
    ----------
    config : dict or str
        Path string to configuration file or dictionary
        containing configuration values
    storage_options : dict
        Storage options for reading config file if specified

    Returns
    -------
    dict
        Configuration dictionary for pipeline
    """
    if isinstance(config, str):
        if not config.endswith(('.yaml', '.yml')):
            raise ValueError("Configuration file must be a YAML!")
        file_system = extract_fs(config, storage_options)
        with file_system.open(config, 'rb') as yaml_file:
            return yaml.safe_load(yaml_file.read())
    return config


def setup_config(
    input_config: Dict[Any, Any], parameters: Dict[Any, Any]
) -> MainConfig:
    """
    Validate configuration dictionary,
    and updating url parameters for harvest

    Parameters
    ----------
    input_config : dict
        Pipeline configuration
    parameters : dict
        Parameters input to overwrite the parameters,
        within the input_config

    Returns
    -------
    MainConfig
        Updated configuration object
    """
    logger = get_run_logger()
    logger.info("Validating configurations ...")
    config = MainConfig(**input_config)
    config.args.parameters.update(parameters)
    logger.info("Configurations validation completed.")
    return config


@task
def init_configuration(
    config: Union[Dict[str, Any], str],
    parameters: Dict[Any, Any] = {},
    storage_options: Dict[str, Any] = {},
) -> MainConfig:
    """
    Task to perform configuration extraction, intialization and validation

    Parameters
    ----------
    config : dict or str
        Path string to configuration file or dictionary
        containing configuration values
    parameters : dict
        Input parameters for creating the full url path.
        *These inputs will overwrite the parameters,
        within the config.*
    storage_options : dict
        Storage options for reading config file if specified

    Returns
    -------
    MainConfig
        Updated configuration object
    """
    config = check_and_extract_config(
        config=config, storage_options=storage_options
    )
    return setup_config(input_config=config, parameters=parameters)
