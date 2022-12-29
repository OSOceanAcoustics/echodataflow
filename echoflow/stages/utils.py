from typing import Any, Dict, Union

from prefect import task

import yaml
from ..subflows.utils import extract_fs


@task
def check_and_extract_config(
    config: Union[Dict[str, Any], str], storage_options: Dict[str, Any] = {}
) -> Dict[str, Any]:
    """
    Task for reading config file if it's yaml

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
