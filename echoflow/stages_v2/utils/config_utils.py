import json
from typing import Any, Dict, List, Literal, Optional, Tuple, Union
from urllib.parse import urlparse
import fsspec
import yaml
import itertools as it
import os
from dateutil import parser
import re
from zipfile import ZipFile
import logging

from echoflow.config.models.dataset import Dataset, StorageOptions

from prefect import task

TRANSECT_FILE_REGEX = r"x(?P<transect_num>\d+)"


@task
def extract_config(
    config: Union[Dict[str, Any], str], storage_options: Dict[str, Any] = {}
) -> Dict[str, Any]:
    file_system = extract_fs(config, storage_options)
    with file_system.open(config, "rb") as yaml_file:
        return yaml.safe_load(yaml_file.read())


@task
def check_config(dataset_config: Dict[str, Any], pipeline_config: Dict[str, Any]):

    if not "active_recipe" in pipeline_config:
        raise ValueError("Pipeline Configuration must have active recipe name!")
    else:
        recipe_name = pipeline_config["active_recipe"]
        recipe_list = pipeline_config["pipeline"]
        recipe_names = []
        for recipe in recipe_list:
            recipe_names.append(recipe["recipe_name"])
        if not recipe_name in recipe_names:
            raise ValueError("Active recipe name not found in the recipe list!")


@task
def glob_all_files(config: Dataset) -> List[str]:
    """
    Task for fetch individual file urls from a source path,
    defined in Dataset class

    Parameters
    ----------
    config : Dataset
        Dataset configuration

    Returns
    -------
    list
        List of raw url paths string
    """

    total_files = []
    data_path = config.args.rendered_path
    storage_options = config.args.storage_options

    if data_path is not None:
        if isinstance(data_path, list):
            for path in data_path:
                all_files = glob_url(path, dict(storage_options))
                total_files.append(all_files)
            total_files = list(it.chain.from_iterable(total_files))
        else:
            total_files = glob_url(data_path, dict(storage_options))
    return total_files


def glob_url(path: str, storage_options: StorageOptions) -> List[str]:
    """
    Glob files based on given path string,
    using fsspec.filesystem.glob

    Parameters
    ----------
    path : str
        The url path string glob pattern
    **storage_options : StorageOptions
        Extra arguments to pass to the filesystem class,
        to allow permission to glob

    Returns
    -------
    List of paths from glob
    """

    file_system, scheme = extract_fs(path, dict(storage_options), include_scheme=True)
    all_files = [f if f.startswith(scheme) else f"{scheme}://{f}" for f in file_system.glob(path)]
    return all_files


def extract_fs(
    url: str,
    storage_options: Dict[Any, Any] = {},
    include_scheme: bool = False,
) -> Union[Tuple[Any, str], Any]:
    """
    Extract the fsspec file system from a url path.

    Parameters
    ----------
    url : str
        The url path string to be parsed and file system extracted
    storage_options : dict
        Additional keywords to pass to the filesystem class
    include_scheme : bool
        Flag to include scheme in the output of the function

    Returns
    -------
    Filesystem for given protocol and arguments
    """

    parsed_path = urlparse(url)
    file_system = fsspec.filesystem(parsed_path.scheme, **storage_options)
    if include_scheme:
        return file_system, parsed_path.scheme
    return file_system


@task
def parse_raw_paths(all_raw_files: List[str], config: Dataset) -> List[Dict[Any, Any]]:
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
    sonar_model = config.sonar_model
    fname_pattern = config.raw_regex
    transect_dict = {}
    if config.args.transect is not None:
        # When transect info is available, extract it
        file_input = config.args.transect.file
        storage_options = config.args.transect.storage_options
        if isinstance(file_input, str):
            filename = os.path.basename(file_input)
            _, ext = os.path.splitext(filename)
            transect_dict = extract_transect_files(ext.strip("."), file_input, storage_options)
        else:
            transect_dict = {}
            for f in file_input:
                filename = os.path.basename(f)
                _, ext = os.path.splitext(filename)
                result = extract_transect_files(ext.strip("."), f, storage_options)
                transect_dict.update(result)

    raw_file_dicts = []
    for raw_file in all_raw_files:
        # get transect info from the transect_dict above
        transect = transect_dict.get(os.path.basename(raw_file), {})
        transect_num = transect.get("num", None)
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
    return raw_file_dicts


def extract_transect_files(
    file_format: Literal["txt", "zip"],
    file_path: str,
    storage_options: StorageOptions,
) -> Dict[str, Dict[str, Any]]:
    """
    Extracts raw file names and transect number from transect file(s)

    Parameters
    ----------
    file_format : str
        Transect file format. Options are 'txt' or 'zip' only.
    file_path : str
        The full path to the transect file.
    storage_options : dict
        Storage options for transect file access.

    Returns
    -------
    dict
        Raw file name dictionary with the original transect file name
        and transect number. For example:

        ```
        {
            'Summer2017-D20170627-T171516.raw': {
                'filename': 'x0007_fileset.txt',
                'num': 7
            },
            'Summer2017-D20170627-T174838.raw': {
                'filename': 'x0007_fileset.txt',
                'num': 7
            }
        }
        ```

    """
    file_system = extract_fs(file_path, storage_options=dict(storage_options))

    if file_format == "zip":
        return _extract_from_zip(file_system, file_path)
    elif file_format == "txt":
        return _extract_from_text(file_system, file_path)
    else:
        raise ValueError(f"Invalid file format: {file_format}. Only 'txt' or 'zip' are valid")


def _extract_from_zip(file_system, file_path: str) -> Dict[str, Dict[str, Any]]:
    transect_dict = {}
    """Extracts raw files from transect file zip format"""
    with file_system.open(file_path, "rb") as f:
        with ZipFile(f) as zf:
            zip_infos = zf.infolist()
            for zi in zip_infos:
                m = re.match(TRANSECT_FILE_REGEX, zi.filename)
                transect_num = int(m.groupdict()["transect_num"])
                if zi.is_dir():
                    raise ValueError("Directory found in zip file. This is not allowed!")
                with zf.open(zi.filename) as txtfile:
                    file_list = txtfile.read().decode("utf-8").split("\r\n")
                    for rawfile in file_list:
                        transect_dict.setdefault(
                            rawfile,
                            {"filename": zi.filename, "num": transect_num},
                        )
    return transect_dict


def _extract_from_text(file_system, file_path: str) -> Dict[str, Dict[str, Any]]:
    """Extracts raw files from transect file text format"""
    filename = os.path.basename(file_path)
    m = re.match(TRANSECT_FILE_REGEX, filename)
    transect_num = int(m.groupdict()["transect_num"])

    transect_dict = {}

    with file_system.open(file_path) as txtfile:
        file_list = txtfile.read().decode("utf-8").split("\r\n")
        for rawfile in file_list:
            transect_dict.setdefault(
                rawfile,
                {"filename": filename, "num": transect_num},
            )

    return transect_dict


def parse_file_path(raw_file: str, fname_pattern: str) -> Dict[str, Any]:
    """
    Parses file path to get at the datetime

    Parameters
    ----------
    raw_file : str
        Raw file url string
    fname_pattern : str
        Regex pattern string for date extraction from file

    Returns
    -------
    dict
        Raw file url dictionary that contains parsed dates
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
def club_raw_files(
    config: Dataset,
    raw_dicts: List[Dict[str, Any]] = [],
    raw_url_file: Optional[str] = None,
    json_storage_options: Dict[Any, Any] = {},
) -> List[List[Dict[str, Any]]]:
    """
    Task to parse raw urls json files and splits them into
    weekly list by utilizing julian days.

    This assumes the following raw url dictionary

    ```
    {'instrument': 'EK60',
    'file_path': 'https://example.com/some-file.raw',
    'month': 1,
    'year': 2017,
    'jday': 1,
    'datetime': '2017-01-01T00:00:00'}
    ```

    Parameters
    ----------
    config : dict
        Pipeline configuration file
    raw_dicts : list, optional
        List of raw url dictionary
    raw_url_file : str, optional
        Raw urls file path string
    json_storage_options : dict
        Storage options for reading raw urls file path

    Returns
    -------
    List of list of raw urls string,
    broken up to 7 julian days each chunk
    """
    if len(raw_dicts) == 0:
        if raw_url_file is None:
            raise ValueError("Must have raw_dicts or raw_url_file present.")
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
            all_jdays[i : i + n] for i in range(0, len(all_jdays), n)
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


    updated_config = {}
    for key, value in prefect_config.items():
        if type(value) == str and '(' in value and ')' in value:
            class_name, params_str = value.split('(', 1)
            params_str = params_str.rstrip(')')
            parameters = {}
            for param in params_str.split(','):
                param_key, param_value = param.split('=')
                parameters[param_key.strip()] = param_value.strip()
            class_object = globals()[class_name](**parameters)
            updated_config[key] = class_object
        else:
            updated_config[key] = value
    return updated_config