from typing import Dict, Any, List
import itertools as it
import os

import fsspec
from prefect import get_run_logger, task

from ..utils import glob_url
from .utils import parse_file_path, extract_transect_files


@task
def glob_all_files(config: Dict[Any, Any]) -> List[str]:
    """
    Task for fetch individual file urls from a source path,
    defined in config dictionary

    Parameters
    ----------
    config : dict
        Pipeline configuration

    Returns
    -------
    list
        List of raw url paths string
    """
    logger = get_run_logger()
    logger.info("Fetching raw file paths ...")
    total_files = []
    data_path = config.args.rendered_path
    storage_options = config.args.storage_options
    logger.info(f"File pattern: {data_path}.")
    if data_path is not None:
        if isinstance(data_path, list):
            for path in data_path:
                all_files = glob_url(path, **storage_options)
                total_files.append(all_files)
            total_files = list(it.chain.from_iterable(total_files))
        else:
            total_files = glob_url(data_path, **storage_options)
    logger.info(f"There are {len(total_files)} raw files.")
    return total_files


@task
def parse_raw_paths(
    all_raw_files: List[str], config: Dict[Any, Any]
) -> List[Dict[Any, Any]]:
    """
    Task for parsing raw url paths,
    extracting info from it,
    and creating a file dictionary

    Parameters
    ----------
    all_raw_files : list
        List of raw url paths string
    config : dict
        Pipeline configuration

    Returns
    -------
    list
        List of raw url paths dictionary
    """
    logger = get_run_logger()
    logger.info("Parsing file paths into dictionary ...")
    sonar_model = config.sonar_model
    fname_pattern = config.raw_regex
    transect_dict = {}
    if config.args.transect is not None:
        # When transect info is available, extract it
        logger.info("Transect file provided. Extracting ...")
        file_input = config.args.transect.file
        storage_options = config.args.transect.storage_options
        if isinstance(file_input, str):
            filename = os.path.basename(file_input)
            _, ext = os.path.splitext(filename)
            transect_dict = extract_transect_files(
                ext.strip('.'), file_input, storage_options
            )
        else:
            transect_dict = {}
            for f in file_input:
                filename = os.path.basename(f)
                _, ext = os.path.splitext(filename)
                result = extract_transect_files(
                    ext.strip('.'), f, storage_options
                )
                transect_dict.update(result)

    raw_file_dicts = []
    for raw_file in all_raw_files:
        # get transect info from the transect_dict above
        transect = transect_dict.get(os.path.basename(raw_file), {})
        transect_num = transect.get('num', None)
        if (config.args.transect is None) or (
            config.args.transect is not None and transect_num is not None
        ):
            # Only adds to the list if not transect
            # if it's a transect, ensure it has a transect number
            raw_file_dicts.append(
                dict(
                    instrument=sonar_model,
                    file_path=raw_file,
                    transect_num=transect_num,
                    **parse_file_path(raw_file, fname_pattern),
                )
            )
    logger.info("Finished creating list of raw file dicts.")
    return raw_file_dicts


@task
def export_raw_dicts(
    raw_dicts: List[Dict[Any, Any]],
    export_path: str,
    export_storage_options: Dict[Any, Any] = {},
):
    """
    Task for exporting the raw url paths dictionary to a json file

    Parameters
    ----------
    raw_dicts : list
        List of raw url paths dictionary
    export_path : str
        Full path to JSON file for saving
    export_storage_options : dict
        Storage options for destination to store paths file

    Returns
    -------
    None
    """
    import json

    json_str = json.dumps(raw_dicts)
    with fsspec.open(export_path, mode="wt", **export_storage_options) as f:
        f.write(json_str)
