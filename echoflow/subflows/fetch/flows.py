from typing import Dict, Any, Tuple, List
from prefect import flow

from .tasks import (
    export_raw_dicts,
    glob_all_files,
    parse_raw_paths,
)
from ...settings.models import MainConfig


@flow
def find_raw_pipeline(
    config: MainConfig,
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
    total_files = glob_all_files(config)
    file_dicts = parse_raw_paths(total_files, config)

    if export:
        export_raw_dicts(file_dicts, export_path, export_storage_options)
    return file_dicts
