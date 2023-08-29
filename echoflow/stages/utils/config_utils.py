"""
This module contains utility functions for various tasks, such as configuration extraction, file processing, and more.

Functions:
    extract_config(
        config: Union[Dict[str, Any], str], storage_options: Dict[str, Any] = {}
    ) -> Dict[str, Any]
    check_config(dataset_config: Dict[str, Any], pipeline_config: Dict[str, Any])
    glob_url(path: str, storage_options: Dict[str, Any]={}) -> List[str]
    extract_transect_files(
        file_format: Literal["txt", "zip"],
        file_path: str,
        storage_options: Dict[str, Any] = {},
    ) -> Dict[str, Dict[str, Any]]
    parse_file_path(raw_file: str, fname_pattern: str) -> Dict[str, Any]
    get_prefect_config_dict(stage: Stage, pipeline: Recipe, prefect_config_dict: Dict[str, Any])
    glob_all_files(config: Dataset) -> List[str]
    parse_raw_paths(all_raw_files: List[str], config: Dataset) -> List[Dict[Any, Any]]
    club_raw_files(
        config: Dataset,
        raw_dicts: List[Dict[str, Any]] = [],
        raw_url_file: Optional[str] = None,
        json_storage_options: StorageOptions = None
    ) -> List[List[Dict[str, Any]]]
    get_storage_options(storage_options: Block = None) -> Dict[str, Any]
    load_block(name: str = None, type: StorageType =  None)

Author: Soham Butala
Email: sbutala@uw.edu
Date: August 22, 2023
"""
import asyncio
import nest_asyncio
import json
from typing import Any, Coroutine, Dict, List, Literal, Optional, Union
from prefect_aws import AwsCredentials
from prefect_azure import AzureCosmosDbCredentials
import yaml
import itertools as it
import os
from dateutil import parser
import re
from zipfile import ZipFile

from echoflow.config.models.datastore import Dataset, StorageOptions, StorageType

from prefect import task
from prefect.filesystems import *
from prefect.task_runners import *
from echoflow.config.models.pipeline import Recipe, Stage

from echoflow.stages.aspects.echoflow_aspect import echoflow
from echoflow.stages.utils.file_utils import make_temp_folder, extract_fs


TRANSECT_FILE_REGEX = r"x(?P<transect_num>\d+)"
nest_asyncio.apply(asyncio.get_event_loop())


@task
def extract_config(
    config: Union[Dict[str, Any], str], storage_options: Dict[str, Any] = {}
) -> Dict[str, Any]:
    """
    Extracts configuration from YAML file.

    Args:
        config (Union[Dict[str, Any], str]): Configuration data or path to YAML file.
        storage_options (Dict[str, Any], optional): Storage options for file access. Defaults to {}.

    Returns:
        Dict[str, Any]: Extracted configuration.

    Example:
        config_dict = extract_config("config.yaml")
    """
    file_system = extract_fs(config, storage_options)
    with file_system.open(config, "rb") as yaml_file:
        return yaml.safe_load(yaml_file.read())


@task
def check_config(dataset_config: Dict[str, Any], pipeline_config: Dict[str, Any]):
    """
    Checks if the provided pipeline configuration is valid.

    Args:
        dataset_config (Dict[str, Any]): Dataset configuration.
        pipeline_config (Dict[str, Any]): Pipeline configuration.

    Raises:
        ValueError: If active recipe name is missing in pipeline configuration,
                    or if active recipe name is not found in the recipe list.

    Example:
        dataset_config = {...}
        pipeline_config = {...}
        check_config(dataset_config, pipeline_config)
    """
    if not "active_recipe" in pipeline_config:
        raise ValueError(
            "Pipeline Configuration must have active recipe name!")
    else:
        recipe_name = pipeline_config["active_recipe"]
        recipe_list = pipeline_config["pipeline"]
        recipe_names = []
        for recipe in recipe_list:
            recipe_names.append(recipe["recipe_name"])
        if not recipe_name in recipe_names:
            raise ValueError(
                "Active recipe name not found in the recipe list!")


def glob_url(path: str, storage_options: Dict[str, Any] = {}) -> List[str]:
    """
    Glob files based on the given path string using fsspec.filesystem.glob.

    Args:
        path (str): The URL path string glob pattern.
        storage_options (Dict[str, Any], optional): Extra arguments for the filesystem class.
                                                     Defaults to {}.

    Returns:
        List[str]: List of paths from the glob.

    Example:
        files = glob_url("s3://my-bucket/data/*.txt", {"anon": True})
    """

    file_system, scheme = extract_fs(
        path, storage_options, include_scheme=True)
    all_files = [f if f.startswith(
        scheme) else f"{scheme}://{f}" for f in file_system.glob(path)]
    return all_files


def extract_transect_files(
    file_format: Literal["txt", "zip"],
    file_path: str,
    storage_options: Dict[str, Any] = {},
    default_transect: int = 0
) -> Dict[str, Dict[str, Any]]:
    """
    Extracts raw file names and transect numbers from transect file(s).

    Args:
        file_format (Literal["txt", "zip"]): Transect file format (txt or zip).
        file_path (str): The full path to the transect file.
        storage_options (dict): Storage options for transect file access.
        default_transect: int = Default transect in case of no transect specified. Defaults to 0

    Returns:
        Dict[str, Dict[str, Any]]: Raw file name dictionary with transect information.

    Example:
        file_info = extract_transect_files("zip", "s3://my-bucket/transects.zip", {"anon": True})
    """
    file_system = extract_fs(file_path, storage_options=storage_options)

    if file_format == "zip":
        return _extract_from_zip(file_system, file_path)
    elif file_format == "txt":
        return _extract_from_text(file_system, file_path, default_transect)
    else:
        raise ValueError(
            f"Invalid file format: {file_format}. Only 'txt' or 'zip' are valid")


def _extract_from_zip(file_system, file_path: str) -> Dict[str, Dict[str, Any]]:
    """
    Extracts raw files from transect file zip format.

    Parameters:
        file_system: The filesystem where the zip file is located.
        file_path (str): The path to the transect zip file.

    Returns:
        Dict[str, Dict[str, Any]]: A dictionary containing extracted raw file information.

    Raises:
        ValueError: If a directory is found in the zip file.

    Example:
        file_system = ...
        file_path = 'path/to/transect_file.zip'
        extracted_data = _extract_from_zip(file_system, file_path)
    """
    transect_dict = {}
    with file_system.open(file_path, "rb") as f:
        with ZipFile(f) as zf:
            zip_infos = zf.infolist()
            for zi in zip_infos:
                m = re.match(TRANSECT_FILE_REGEX, zi.filename)
                transect_num = int(m.groupdict()["transect_num"])
                if zi.is_dir():
                    raise ValueError(
                        "Directory found in zip file. This is not allowed!")
                with zf.open(zi.filename) as txtfile:
                    file_list = txtfile.read().decode("utf-8").split("\r\n")
                    for rawfile in file_list:
                        transect_dict.setdefault(
                            rawfile,
                            {"filename": zi.filename, "num": transect_num},
                        )
    return transect_dict


def _extract_from_text(file_system, file_path: str, default_transect: int = 0) -> Dict[str, Dict[str, Any]]:
    """
    Extracts raw files from transect file text format.

    Parameters:
        file_system: The filesystem where the text file is located.
        file_path (str): The path to the transect text file.
        default_transect: int = Default transect in case of no transect specified. Defaults to 0


    Returns:
        Dict[str, Dict[str, Any]]: A dictionary containing extracted raw file information.

    Example:
        file_system = ...
        file_path = 'path/to/transect_file.txt'
        extracted_data = _extract_from_text(file_system, file_path)
    """
    filename = os.path.basename(file_path)
    m = re.match(TRANSECT_FILE_REGEX, filename)
    if m is None:
        transect_num = default_transect
    else:
        transect_num = int(m.groupdict()["transect_num"])

    transect_dict = {}

    with file_system.open(file_path) as txtfile:
        file_list = txtfile.read().decode("utf-8").splitlines()
        for rawfile in file_list:
            transect_dict.setdefault(
                rawfile,
                {"filename": filename, "num": transect_num},
            )

    return transect_dict


def parse_file_path(raw_file: str, fname_pattern: str) -> Dict[str, Any]:
    """
    Parses file path to extract datetime information.

    Parameters:
        raw_file (str): The raw file URL.
        fname_pattern (str): The regex pattern for date extraction.

    Returns:
        Dict[str, Any]: A dictionary containing parsed date and time information.

    Example:
        raw_file = 'https://example.com/file_2023_0815.txt'
        fname_pattern = r'file_(?P<year>\d{4})_(?P<month>\d{2})(?P<day>\d{2})'
        parsed_data = parse_file_path(raw_file, fname_pattern)
    """
    matcher = re.compile(fname_pattern)
    file_match = matcher.search(raw_file)
    match_dict = file_match.groupdict()
    file_datetime = None
    if "date" in match_dict and "time" in match_dict:
        datetime_obj = parser.parse(f"{file_match['date']}{file_match['time']}")  # noqa
        file_datetime = datetime_obj.isoformat()
        jday = datetime_obj.timetuple().tm_yday
        match_dict.pop("date")
        match_dict.pop("time")
        match_dict.setdefault("month", datetime_obj.month)
        match_dict.setdefault("year", datetime_obj.year)
        match_dict.setdefault("jday", jday)

    match_dict.setdefault("datetime", file_datetime)
    return dict(**match_dict)


@task
def get_prefect_config_dict(stage: Stage, pipeline: Recipe, prefect_config_dict: Dict[str, Any]):
    """
    Gets the updated Prefect configuration dictionary.

    Parameters:
        stage (Stage): The Prefect stage.
        pipeline (Recipe): The Prefect pipeline.
        prefect_config_dict (Dict[str, Any]): The Prefect configuration dictionary.

    Returns:
        Dict[str, Any]: The updated Prefect configuration dictionary.

    Example:
        stage = ...
        pipeline = ...
        prefect_config_dict = {...}
        updated_config = get_prefect_config_dict(stage, pipeline, prefect_config_dict)
    """
    prefect_config: Dict[str, Any] = stage.prefect_config
    updated_config = {}
    if (stage.prefect_config is not None):
        for key, value in prefect_config.items():
            if type(value) == str and '(' in value and ')' in value:
                class_name, params_str = value.split('(', 1)
                params_str = params_str.rstrip(')')
                parameters = {}
                if params_str is not None and params_str != '':
                    for param in params_str.split(','):
                        param_key, param_value = param.split('=')
                        parameters[param_key.strip()] = param_value.strip()
                class_object = globals()[class_name](**parameters)
                updated_config[key] = class_object
            else:
                updated_config[key] = value
    return updated_config


@task
@echoflow(processing_stage="Configuration", type="CONFIG_TASK")
def glob_all_files(config: Dataset) -> List[str]:
    """
    Fetches individual file URLs from a source path in the Dataset configuration.

    Parameters:
        config (Dataset): The Dataset configuration.

    Returns:
        List[str]: A list of raw URL paths.

    Example:
        dataset_config = ...
        raw_urls = glob_all_files(dataset_config)
    """
    total_files = []
    data_path = config.args.rendered_path
    storage_options = config.args.storage_options_dict
    if data_path is not None:
        if isinstance(data_path, list):
            for path in data_path:
                all_files = glob_url(path, dict(storage_options))
                total_files.append(all_files)
            total_files = list(it.chain.from_iterable(total_files))
        else:
            total_files = glob_url(data_path, dict(storage_options))

    return total_files


@task
@echoflow(processing_stage="Configuration", type="CONFIG_TASK")
def parse_raw_paths(all_raw_files: List[str], config: Dataset) -> List[Dict[Any, Any]]:
    """
    Parses raw URL paths, extracts information, and creates a file dictionary.

    Parameters:
        all_raw_files (List[str]): List of raw URL paths.
        config (Dataset): The Pipeline configuration.

    Returns:
        List[Dict[Any, Any]]: List of dictionaries containing parsed raw URL information.

    Example:
        all_raw_files = ['https://example.com/file1.txt', 'https://example.com/file2.txt']
        dataset_config = ...
        parsed_data = parse_raw_paths(all_raw_files, dataset_config)
    """
    sonar_model = config.sonar_model
    fname_pattern = config.raw_regex
    transect_dict = {}

    if config.args.transect is not None and config.args.transect.file is not None:
        # When transect info is available, extract it
        file_input = config.args.transect.file
        storage_options = config.args.transect.storage_options_dict
        default_transect = config.args.transect.default_transect_num
        if isinstance(file_input, str):
            filename = os.path.basename(file_input)
            _, ext = os.path.splitext(filename)
            transect_dict = extract_transect_files(
                ext.strip("."), file_input, storage_options, default_transect)
        else:
            transect_dict = {}
            for f in file_input:
                filename = os.path.basename(f)
                _, ext = os.path.splitext(filename)
                result = extract_transect_files(
                    ext.strip("."), f, storage_options, default_transect)
                transect_dict.update(result)

    raw_file_dicts = []
    for raw_file in all_raw_files:
        # get transect info from the transect_dict above
        transect = transect_dict.get(os.path.basename(raw_file), {})
        transect_num = transect.get(
        "num", config.args.transect.default_transect_num)
        if (config.args.transect is None) or (
            config.args.transect is not None and transect_num is not None and bool(transect)
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
    if config.args.json_export:
        out_path = make_temp_folder(
            config.output.urlpath+"/raw_json", config.output.storage_options_dict)
        out_path = out_path+"/"+config.name+".json"
        fs = extract_fs(out_path, config.output.storage_options_dict)
        with fs.open(out_path, mode="w") as f:
            json.dump(raw_file_dicts, f)
    return raw_file_dicts


@task
@echoflow(processing_stage="Configuration", type="CONFIG_TASK")
def club_raw_files(
    config: Dataset,
    raw_dicts: List[Dict[str, Any]] = [],
    raw_url_file: Optional[str] = None,
    json_storage_options: StorageOptions = None
) -> List[List[Dict[str, Any]]]:
    """
    Parses raw URLs, splits them into weekly lists using Julian days.

    Parameters:
        config (Dataset): The Pipeline configuration.
        raw_dicts (List[Dict[str, Any]]): List of raw URL dictionaries.
        raw_url_file (Optional[str]): Path to raw URLs JSON file.
        json_storage_options (StorageOptions): Storage options for reading raw URLs.

    Returns:
        List[List[Dict[str, Any]]]: List of lists of raw URL dictionaries grouped by week.

    Example:
        dataset_config = ...
        raw_dicts = [...]
        raw_url_file = 'path/to/raw_urls.json'
        json_storage_options = ...
        grouped_raw_data = club_raw_files(dataset_config, raw_dicts, raw_url_file, json_storage_options)
    """

    if len(raw_dicts) == 0:
        if raw_url_file is None:
            raise ValueError("Must have raw_dicts or raw_json_path present.")
        file_system = extract_fs(
            raw_url_file, storage_options=json_storage_options
        )
        with file_system.open(raw_url_file) as f:
            raw_dicts = json.load(f)

    if config.args.transect is not None:
        # Transect, split by transect spec
        raw_dct = {}
        for r in raw_dicts:
            transect_num = r['transect_num']
            if transect_num not in raw_dct:
                raw_dct[transect_num] = []
            raw_dct[transect_num].append(r)

        all_files = [
            sorted(raw_list, key=lambda a: a['datetime'])
            for raw_list in raw_dct.values()
        ]
    else:
        # Number of days for a week chunk
        n = 7

        all_jdays = sorted({r.get("jday") for r in raw_dicts})
        split_days = [
            all_jdays[i: i + n] for i in range(0, len(all_jdays), n)
        ]  # noqa

        day_dict = {}
        for r in raw_dicts:
            mint = r.get("jday")
            if mint not in day_dict:
                day_dict[mint] = []
            day_dict[mint].append(r)

        all_files = []
        for week in split_days:
            files = list(it.chain.from_iterable([day_dict[d] for d in week]))
            all_files.append(files)
    return all_files


def get_storage_options(storage_options: Block = None) -> Dict[str, Any]:
    """
    Get storage options from a Block.

    Parameters:
        storage_options (Block, optional): A block containing storage options.

    Returns:
        Dict[str, Any]: Dictionary containing storage options.

    Example:
        aws_credentials = AwsCredentials(...)
        storage_opts = get_storage_options(aws_credentials)
    """
    storage_options_dict: Dict[str, Any] = {}
    if storage_options is not None:
        if isinstance(storage_options, AwsCredentials):
            storage_options_dict["key"] = storage_options.aws_access_key_id
            storage_options_dict["secret"] = storage_options.aws_secret_access_key.get_secret_value(
            )
            if storage_options.aws_session_token:
                storage_options_dict["token"] = storage_options.aws_session_token

    return storage_options_dict


def load_block(name: str = None, type: StorageType = None):
    """
    Load a block of a specific type by name.

    Parameters:
        name (str, optional): The name of the block to load.
        type (StorageType, optional): The type of the block to load.

    Returns:
        block: The loaded block.

    Raises:
        ValueError: If name or type is not provided.

    Example:
        loaded_aws_credentials = load_block(name="my-aws-creds", type=StorageType.AWS)
    """
    if name is None or type is None:
        raise ValueError("Cannot load block without name")

    if type == StorageType.AWS:
        coro = AwsCredentials.load(name=name)
    elif type == StorageType.AZCosmos:
        coro = AzureCosmosDbCredentials.load(name=name)
    
    if isinstance(coro, Coroutine):
        block = nest_asyncio.asyncio.run(coro)
    else:
        block = coro
    return block
