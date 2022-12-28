from typing import Dict, Any, Tuple, List
from prefect import flow

from .tasks import (
    export_raw_dicts,
    glob_all_files,
    parse_raw_paths,
    setup_config,
)


@flow
def find_raw_pipeline(
    config: Dict[Any, Any],
    parameters: Dict[Any, Any] = {},
    export: bool = False,
    export_path: str = "",
    export_storage_options: Dict[Any, Any] = {},
) -> Tuple[List[Dict[Any, Any]], Dict[Any, Any]]:
    """
    Pipeline for finding raw files from a given url location

    Parameters
    ----------
    config : dict
        Pipeline configuration dictionary
    parameters : dict
        Input parameters for creating the full url path.
        *These inputs will overwrite the parameters,
        within the config.*
    export : bool
        Flag to export raw paths for individual raw files
        to a raw urls JSON file
    export_path : str
        Path to store raw urls JSON file if export is True
    export_storage_options: dict
        Storage options for destination to store paths file

    Returns
    -------
    Tuple
        Raw urls dictionary and an updated configuration dictionary

    """
    new_config = setup_config(config, parameters)
    total_files = glob_all_files(new_config)
    file_dicts = parse_raw_paths(total_files, new_config)

    if export:
        export_raw_dicts(file_dicts, export_path, export_storage_options)
    return file_dicts, new_config
