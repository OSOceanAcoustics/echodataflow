"""
This module provides utility functions related to working with files, directories, and data paths.

Module Functions:
    download_temp_file(raw: Dict[str, Any], working_dir: str, stage: Stage, config: Dataset) -> Dict[str, Any]:
        Downloads a temporary raw file from a URL path.

    extract_fs(url: str, storage_options: Dict[Any, Any] = {}, include_scheme: bool = False) -> Union[Tuple[Any, str], Any]:
        Extracts the fsspec file system from a URL path.

    make_temp_folder(folder_name: str, storage_options: Dict[str, Any]) -> str:
        Creates a temporary folder locally or remotely using fsspec.

    get_output_file_path(raw_dicts: List[Dict[str, Any]], config: Dataset) -> str:
        Generates the output file path based on the raw file dictionary's datetime key.

    isFile(file_path: str, storage_options: Dict[str, Any] = {}) -> bool:
        Checks if a file exists at the specified path using the fsspec file system.

    get_working_dir(stage: Stage, config: Dataset) -> str:
        Retrieves the working directory for a stage based on stage options and configuration.

    get_ed_list(config: Dataset, stage: Stage, transect_data: Union[Output, List[Dict[str, Any]], Dict[str, Any]]) -> List[open_converted]:
        Retrieves a list of open_converted objects for echopype data.

    get_zarr_list(config: Dataset, stage: Stage, transect_data: Union[Output, Dict[str, Any]]) -> List[xarray.Dataset]:
        Retrieves a list of xarray.Dataset objects for zarr data.

Author: Soham Butala
Email: sbutala@uw.edu
Date: August 22, 2023
"""
from collections import defaultdict
import json
import os
import platform
from pathlib import Path
from typing import Any, Dict, List, Tuple, Union
from urllib.parse import urlparse

from echodataflow.utils import log_util
import fsspec
import xarray as xr
from dateutil import parser
from echopype import open_converted
from fastapi.encoders import jsonable_encoder
from fsspec.implementations.local import LocalFileSystem
from prefect import task
import pandas as pd

from echodataflow.models.datastore import Dataset
from echodataflow.models.output_model import EchodataflowObject, Group, Output
from echodataflow.models.pipeline import Pipeline, Stage
from echodataflow.utils import log_util


def download_temp_file(raw: EchodataflowObject, working_dir: str, stage: Stage, config: Dataset):
    """
    Downloads a temporary raw file from a URL path.

    Args:
        raw (Dict[str, Any]): Raw file dictionary containing file information.
        working_dir (str): Working directory where the file will be downloaded.
        stage (Stage): Current processing stage object.
        config (Dataset): Dataset configuration.

    Returns:
        Dict[str, Any]: Updated raw file dictionary with local file path.

    Example:
        raw_data = {'file_path': 'https://example.com/data.raw', ...}
        working_directory = '/path/to/working_dir'
        stage_object = ...
        dataset_config = ...
        updated_raw = download_temp_file(raw_data, working_directory, stage_object, dataset_config)
    """

    urlpath = raw.file_path
    fname = os.path.basename(urlpath)
    
    if stage.options["group"] == False:
        out_path = format_windows_path(working_dir+"/raw_files/"+fname, slash=True)
        make_temp_folder(
            format_windows_path(working_dir + "/raw_files/", slash=True),
            config.output.storage_options_dict,
        )
    else: 
        out_path = format_windows_path(
            working_dir + "/" + str(raw.group_name) + "_raw_files/" + fname, slash=True
        )
        make_temp_folder(
            format_windows_path(
                working_dir + "/" + str(raw.group_name) + "_raw_files/", slash=True
            ),
            config.output.storage_options_dict,
        )
    
    print(out_path)
    working_dir_fs = extract_fs(out_path, storage_options=config.output.storage_options_dict)

    if (
        stage.options.get("use_raw_offline") == False
        or isFile(out_path, config.output.storage_options_dict) == False
    ):
        print("Downloading ...", out_path)
        file_system = extract_fs(urlpath, storage_options=config.args.storage_options_dict)
        with file_system.open(urlpath, "rb") as source_file:
            with working_dir_fs.open(out_path, mode="wb") as f:
                f.write(source_file.read())
    raw.local_path = out_path


def format_windows_path(path: str, slash: bool = False):
    if platform.system() == "Windows" and not path.startswith(
        ("file:///", "s3://", "http://", "https://")
    ):
        if slash:
            return path.replace("/", "\\")
        else:
            return "file:///" + path.replace("/", "\\")
    else:
        return path
    
def extract_fs(
    url: str,
    storage_options: Dict[Any, Any] = {},
    include_scheme: bool = False,
) -> Union[Tuple[Any, str], Any]:
    """
    Extracts the fsspec file system from a URL path.

    Args:
        url (str): URL path string.
        storage_options (Dict[Any, Any]): Additional keywords to pass to the filesystem class.
        include_scheme (bool): Flag to include scheme in the output of the function.

    Returns:
        Union[Tuple[Any, str], Any]: Filesystem for the given protocol and arguments.

    Example:
        fs = extract_fs('s3://mybucket/data', storage_options={'anon': True})
    """
    parsed_path = urlparse(format_windows_path(url))
    file_system = fsspec.filesystem(parsed_path.scheme, **storage_options)
    if include_scheme:
        return file_system, parsed_path.scheme
    return file_system

def make_temp_folder(folder_name: str, storage_options: Dict[str, Any]) -> str:
    """
    Creates a temporary folder locally or remotely using fsspec.

    Args:
        folder_name (str): Name of the folder.
        storage_options (Dict[str, Any]): Storage options for fsspec.

    Returns:
        str: Path to the created temporary folder.

    Example:
        temp_folder = make_temp_folder('temp_folder', storage_options={'anon': True})
    """    
    fsmap = fsspec.get_mapper(folder_name, **storage_options)
    if fsmap.fs.isdir(fsmap.root) == False:
        fsmap.fs.mkdir(fsmap.root, exists_ok=True, create_parents=True)
    if isinstance(fsmap.fs, LocalFileSystem):
        return str(Path(folder_name).resolve())
    return folder_name

@task
def get_output_file_path(raw_dicts, config: Dataset):
    """
    Get the output file path based on the raw file dictionary datetime key.
    It will grab the first file and use that as the file name in the form of 'DYYYYmmdd-THHMMSS.zarr'.

    This method will also grab a value from the config.output.urlpath for where the file should go.

    Parameters:
        raw_dicts (List[Dict[str, Any]]): The list of raw URL dictionaries. Must have 'datetime' key!
        config (Dataset): The pydantic Dataset object that contains all the necessary configurations.

    Returns:
        str: The full path string to the zarr file.

    Example:
        raw_files = [{'datetime': '2023-08-15T12:34:56', ...}, ...]
        dataset_config = ...
        zarr_file_path = get_output_file_path(raw_files, dataset_config)
    """
    if len(raw_dicts) < 1:
        raise ValueError("There must be at least one raw file dictionary!")

    first_file = raw_dicts[0]
    datetime_obj = parser.parse(first_file.get("datetime"))
    if config.args.group is None:
        # Only use datetime
        out_fname = datetime_obj.strftime("D%Y%m%d-T%H%M%S.zarr")
    else:
        # Use transect number
        transect_num = first_file.get("transect_num", None)
        date_name = datetime_obj.strftime("D%Y%m%d-T%H%M%S")
        out_fname = f"x{transect_num:04}-{date_name}.zarr"
    return format_windows_path("/".join([config.output.urlpath, out_fname]))

def isFile(file_path: str, storage_options: Dict[str, Any] = {}):
    """
    Check if a file exists at the specified path using the fsspec file system.

    Parameters:
        file_path (str): The path to the file.
        storage_options (Dict[str, Any]): Storage options for fsspec.

    Returns:
        bool: True if the file exists, False otherwise.

    Example:
        exists = isFile('s3://mybucket/data.zarr', storage_options={'anon': True})
    """
    fs = extract_fs(file_path, storage_options=storage_options)
    if file_path.endswith(".zarr"):
        return fs.isdir(file_path)
    return fs.isfile(file_path)

def get_working_dir(stage: Stage, config: Dataset):
    """
    Get the working directory for a stage based on stage options and configuration.

    Parameters:
        stage (Stage): The current processing stage object.
        config (Dataset): The Dataset configuration.

    Returns:
        str: The path to the working directory.

    Example:
        stage_object = ...
        dataset_config = ...
        working_directory = get_working_dir(stage_object, dataset_config)
    """
    if stage.options is not None and stage.options.get("out_path") is not None:
        working_dir = make_temp_folder(
            stage.options.get("out_path"), config.output.storage_options_dict
        )
    elif config.output.urlpath is not None:
        working_dir = make_temp_folder(
            config.output.urlpath + "/" + stage.name, config.output.storage_options_dict
        )
    else:
        working_dir = make_temp_folder(
            "Echodataflow_working_dir" + "/" + stage.name, config.output.storage_options_dict
        )

    return working_dir

@task
def get_ed_list(
    config: Dataset,
    stage: Stage,
    transect_data: Union[EchodataflowObject, List[EchodataflowObject]],
):
    """
    Get a list of open_converted objects for echopype data.

    Parameters:
        config (Dataset): The Dataset configuration.
        stage (Stage): The current processing stage object.
        transect_data (Union[Output, List[Dict[str, Any]], Dict[str, Any]]): The transect data or output.

    Returns:
        List[open_converted]: A list of open_converted objects.

    Example:
        dataset_config = ...
        stage_object = ...
        transect_output = ...
        echopype_list = get_ed_list(dataset_config, stage_object, transect_output)
    """
    ed_list = []
    if type(transect_data) == list:
        for zarr_path_data in transect_data:
            if zarr_path_data.data:
                ed = zarr_path_data.data
                del zarr_path_data.data
                zarr_path_data.data = None
            ed = open_converted(
                converted_raw_path=str(zarr_path_data.out_path),
            storage_options=dict(config.output.storage_options_dict),
        )
        ed_list.append(ed)
    else:
        if transect_data.data:
            ed = transect_data.data
            del transect_data.data
            transect_data.data = None
        else:
            ed = open_converted(
                converted_raw_path=str(transect_data.out_path),
                storage_options=dict(config.output.storage_options_dict),
            )
        ed_list.append(ed)
    return ed_list

@task
def get_zarr_list(
    transect_data: Union[EchodataflowObject, List[EchodataflowObject]],
    storage_options: Dict[str, Any] = {},
    delete_from_edf: bool = True
) -> List[xr.Dataset]:
    """
    Get a list of xarray.Dataset objects for zarr data.

    Parameters:
        config (Dataset): Output storage options
        transect_data (Union[Output, Dict[str, Any]]): The transect data or output.

    Returns:
        List[xarray.Dataset]: A list of xarray.Dataset objects.

    Example:
        storage_options = ...
        transect_output = ...
        zarr_list = get_zarr_list(transect_output, storage_options)
    """
    zarr_list = []
    if type(transect_data) == list:
        for td in transect_data:
            if td.data:
                zarr = td.data
                if delete_from_edf:
                    del td.data
                td.data = None
            else:
                zarr = xr.open_zarr(td.out_path, storage_options=storage_options)
        zarr_list.append(zarr)
    else:
        if transect_data.data:
            zarr = transect_data.data
            if delete_from_edf:
                del transect_data.data
            transect_data.data = None
        else:
            zarr = xr.open_zarr(transect_data.out_path, storage_options=storage_options)
        zarr_list.append(zarr)

    return zarr_list


def process_output_groups(
    name: str,
    config: Dataset,
    stage: Stage,
    groups: Dict[str, Group],
    error_groups: Dict[str, Group],
) -> List[Output]:
    """
    Process and aggregate output transects.

    This function processes a list of echodata (ed) dictionaries and aggregates them based on transect numbers.
    It raises a ValueError if any of the echodata dictionaries have an error flag set to True.

    Parameters:
        name (str): The name of the process.
        ed_list (List[Dict[str, Any]]): A list of echodata dictionaries.
        config (Dataset): Datastore configuration

    Returns:
        List[Output]: A list of Output instances, each containing aggregated data for a transect.

    Raises:
        ValueError: If any echodata dictionary has an error flag set to True.

    Example:
        ed_list = [
            {"transect": 1, "data": {...}, "error": False},
            {"transect": 1, "data": {...}, "error": False},
            {"transect": 2, "data": {...}, "error": False},
            {"transect": 2, "data": {...}, "error": False},
            {"transect": 2, "data": {...}, "error": True}
        ]

        output_list = process_output_groups("Data Processing", ed_list)
        # Returns a list of Output instances with aggregated data per transect.

    """
    error_flag = False

    # updated_output = Output()
    # for edf in resultList:

    #     g = updated_output.group.get(edf.group_name, Group())
    #     g.group_name = edf.group_name
    #     g.instrument = group.get(edf.group_name).instrument
    #     g.data.append(edf)

    #     updated_output.group[edf.group_name] = g

    #     if edf.error and edf.error.errorFlag:
    #         error_flag = True
    #         error_description = str(edf.error.error_desc)
    #         file = str(edf.filename)
    #         log_util.log(msg={'msg':f'Encountered Some Error in {file}', 'mod_name':__file__, 'func_name':'file_utils'}, use_dask=stage.options['use_dask'], eflogging=config.logging)
    #         log_util.log(msg={'msg':error_description, 'mod_name':__file__, 'func_name':'file_utils'}, use_dask=stage.options['use_dask'], eflogging=config.logging)
    #         error_groups[edf.group_name] = g
    error = None
    error_type = None
    for _, gr in groups.items():
        for ed in gr.data:
            if ed.error and ed.error.errorFlag == True:
                error_flag = True
                
                if error_type:
                    if error_type == "INTERNAL":
                        error_type = ed.error.error_type
                        error = ed.error.error_desc
                else:
                    error_type = ed.error.error_type 
                    error = ed.error.error_desc
                    
                file = str(ed.filename)
                log_util.log(
                    msg={
                        "msg": f"Encountered Some Error in {file}",
                        "mod_name": __file__,
                        "func_name": "file_utils",
                    },
                    use_dask=stage.options["use_dask"],
                    eflogging=config.logging,
                )
                log_util.log(
                    msg={"msg": error, "mod_name": __file__, "func_name": "file_utils"},
                    use_dask=stage.options["use_dask"],
                    eflogging=config.logging,
                )
                error_groups[ed.group_name] = gr

    if error_flag and error_type and error_type != "INTERNAL":
        for name, _ in error_groups.items():
            print("Deleting ", groups[name])
            del groups[name]
            
    if len(groups.keys()) == 0 and error_type and error_type != "INTERNAL":
        raise ValueError(f"Some Error Occurred {error}") 
    
    return groups


def process_output_group(name: str, config: Dataset, stage: Stage, group: Group) -> List[Output]:
    """
    Process and aggregate output transects.

    This function processes a list of echodata (ed) dictionaries and aggregates them based on transect numbers.
    It raises a ValueError if any of the echodata dictionaries have an error flag set to True.

    Parameters:
        name (str): The name of the process.
        ed_list (List[Dict[str, Any]]): A list of echodata dictionaries.
        config (Dataset): Datastore configuration

    Returns:
        List[Output]: A list of Output instances, each containing aggregated data for a transect.

    Raises:
        ValueError: If any echodata dictionary has an error flag set to True.

    Example:
        ed_list = [
            {"transect": 1, "data": {...}, "error": False},
            {"transect": 1, "data": {...}, "error": False},
            {"transect": 2, "data": {...}, "error": False},
            {"transect": 2, "data": {...}, "error": False},
            {"transect": 2, "data": {...}, "error": True}
        ]

        output_list = process_output_groups("Data Processing", ed_list)
        # Returns a list of Output instances with aggregated data per transect.

    """

    for ed in group.data:
        if ed.error and ed.error.errorFlag == True:
            error_description = str(ed.error.error_desc)
            file = str(ed.filename)
            log_util.log(
                msg={
                    "msg": f"Encountered Some Error in {file}",
                    "mod_name": __file__,
                    "func_name": "file_utils",
                },
                use_dask=stage.options["use_dask"],
                eflogging=config.logging,
            )
            log_util.log(
                msg={"msg": error_description, "mod_name": __file__, "func_name": "file_utils"},
                use_dask=stage.options["use_dask"],
                eflogging=config.logging,
            )
            return True
    return False


def store_json_output(data, config: Dataset, name: str):
    """
    Store the given data as JSON in the Echodataflow working directory.

    This function serializes the provided `data` and stores it as JSON in the `.echodataflow`
    directory within the user's home directory. The stored JSON data can later be retrieved
    using the `get_output` function.

    Note:
        The stored data can be retrieved using the `get_output` function with the same type
        of data.

    Example:
        >>> output_data = ["data_point_1", "data_point_2", "data_point_3"]
        >>> store_json_output(output_data, config, "output_name")

    Args:
        data (Any): The data to be stored.
        config (Dataset): The configuration dataset.
        name (str): The name of the JSON output file.

    """
    print("Storing JSON Metadata")
    json_data_path = os.path.expanduser(
        os.path.join("~", ".echodataflow", "echodataflow_working_data.json")
    )
    
    if data and isinstance(data, Output):      
        for name, gr in data.group.items():
            for edf in gr.data:
                edf.data = None
                edf.data_ref = None

    serialized_data_list = jsonable_encoder(data)

    # Serialize the list to JSON
    json_data = json.dumps(serialized_data_list)

    with open(json_data_path, "w") as outfile:
        outfile.write(json_data)

    if config.args.json_export:
        out_path = make_temp_folder(
            config.output.urlpath + "/json_metadata", config.output.storage_options_dict
        )
        out_path = out_path + "/" + name + ".json"
        print("Output metdata will be loaded to ",out_path)
        fs = extract_fs(out_path, config.output.storage_options_dict)
        with fs.open(out_path, mode="w") as f:
            f.write(json_data)    
   
def get_output(type : str = "Output"):
    """
    Retrieve stored output data from the Echodataflow working directory.

    This function retrieves previously stored output data from the JSON file located in
    the `.echodataflow` directory within the user's home directory.

    Args:
        type (str, optional): The type of data to retrieve. Defaults to "Output". If a different
        type is specified, the stored raw JSON data is returned without further processing.

    Returns:
        If type is "Output":
            A list of Output instances containing the retrieved data.
        If type is not "Output":
            The raw JSON data as stored, if available.

    Example:
        >>> retrieved_data = get_output(type="custom_type")
        >>> print(retrieved_data)
        {"key": "value", ...}

    """
    data = None    
    json_data_path = os.path.expanduser(
        os.path.join("~", ".echodataflow", "echodataflow_working_data.json")
    )
    with open(json_data_path, "r") as outfile:
        data = json.load(outfile)

    if type != "Output":
        return data  
    output_list = []
    for item in data:
        output_item = Output(**item)
        output_list.append(output_item)
    return output_list


def cleanup(output:Output, config: Dataset, pipeline: Pipeline):
    """
    Clean up working directory associated with a specific stage of processing.

    This function removes the working directory corresponding to the given stage and dataset configuration.

    Parameters:
        config (Dataset): The dataset configuration.
        stage (Stage): The processing stage for which to perform cleanup.
        data (List[Output]): List of Output objects containing information about processed data.

    Example:
        >>> dataset_config = Dataset()
        >>> processing_stage = Stage()
        >>> output_data = [Output(data=[...]), Output(data=[...])]
        >>> cleanup(dataset_config, processing_stage, output_data)
    """
    
    stages = {}
    
    for stage in pipeline.stages:
        
        if stage.options.get("save_offline") == None:
            stages[stage.name] = config.output.retention
        else:
            stages[stage.name] = stage.options.get("save_offline") 
    
    log_util.log(
        msg={
            "msg": f"Stages to cleanup",
            "mod_name": __file__,
            "func_name": "Init Flow",
        },
        eflogging=config.logging,
    )
    
    for _, group in output.group.items():
        for edf in group.data:
            for sname, path in edf.stages.items():
                if sname in stages.keys() and stages[sname] == False:
                    log_util.log(
                        msg={
                            "msg": f"Cleaning {path}",
                            "mod_name": __file__,
                            "func_name": "Init Flow",
                        },
                        eflogging=config.logging,
                    )
                    try:
                        fs = extract_fs(path, storage_options=config.output.storage_options_dict)
                        fs.rm(path, recursive=True)
                    except Exception as e:
                        log_util.log(
                            msg={
                                "msg": f"Failed to cleanup {path}",
                                "mod_name": __file__,
                                "func_name": "Init Flow",
                            },
                            eflogging=config.logging,
                        )
                        log_util.log(
                            msg={
                                "msg": "",
                                "mod_name": __file__,
                                "func_name": "Init Flow",
                            },
                            eflogging=config.logging,
                            error=e
                        )

def get_last_run_output(data: List[Output] = None, storage_options: Dict[str, Any]={}):
    """
    Retrieve Zarr arrays from the last run's output data.

    This function extracts Zarr arrays from the output data of the last run and returns them as a list of lists.

    Parameters:
        data (List[Output], optional): List of Output objects containing information about processed data.
        storage_options (Dict[str, Any], optional): Storage options for loading Zarr arrays.

    Returns:
        List[List]: A list of lists containing Zarr arrays from the last run's output data.

    Example:
        >>> output_data = [Output(data=[...]), Output(data=[...])]
        >>> zarr_arrays = get_last_run_output(output_data, storage_options={"key": "value"})
    """
    outputs : List[List]= []
    if data is None:
        data = get_output()
    if isinstance(data, list) and isinstance(data[0], Output):
        try:
            if isinstance(data[0].data, list):
                for transect in data:
                    ed_list = []
                    for d in transect.data:
                        ed = get_zarr_list(d, storage_options)
                        ed_list.append(ed[0])
                    outputs.append(ed_list)
            else:
                for d in data:
                    ed = get_zarr_list(d, storage_options)
                    ed_list.append(ed[0])
                    outputs.append(ed_list)
            return outputs
        except Exception as e:
            print("Could not load the results.")
            return data
    else:
        return data


def get_out_zarr(
    group: bool, working_dir: str, file_name: str, storage_options: Dict[str, Any], transect: str
) -> str:
    """
    Constructs the output path for a Zarr file based on the provided parameters and storage options.

    Depending on the file system (local or remote) determined by `storage_options` and the presence
    of a group structure (`group` parameter), this function constructs and returns the appropriate 
    file path for storing Zarr datasets.

    Parameters:
        group (bool): Indicates whether the output is part of a group structure. If True, `transect`
                      will be included in the path.
        working_dir (str): The base directory for the output file.
        file_name (str): The name of the file to be generated.
        storage_options (Dict[str, Any]): Options to configure access to the file system, such as credentials.
        transect (str): The name of the transect (group) under which the file should be organized.
                        This is only used if `group` is True.

    Returns:
        str: The fully constructed file path where the Zarr file should be saved.

    Note:
        This function supports both local and remote file systems as determined by `fsspec.get_mapper`
        and the provided `storage_options`.
    """
    fsmap = fsspec.get_mapper(working_dir, **storage_options)
    
    print("File System is : ", fsmap.fs)
    
    if isinstance(fsmap.fs, LocalFileSystem):
        if group:        
            return os.path.join(working_dir, transect, file_name)
        else:
            return os.path.join(working_dir, "zarr_files", file_name)
    else:
        slash_pattern = "/" if "/" in working_dir else "\\"
        if group:
            return slash_pattern.join([working_dir, transect, file_name])
        else:
            return slash_pattern.join([working_dir, "zarr_files", file_name])

def fetch_slice_from_store(edf_group: Group, config: Dataset, options: Dict[str, Any] = None, start_time: str = None, end_time: str = None) -> xr.Dataset:
    default_options = {
                "engine":"zarr",
                "combine":"by_coords",
                "data_vars":"minimal",
                "coords":"minimal",
                "compat":"override",
                "storage_options": config.args.storage_options_dict}
    if options:
        default_options.update(options)
    
    store = xr.open_mfdataset(paths=[ed.out_path for ed in edf_group.data], **default_options).compute()
    store_slice = store.sel(ping_time=slice(pd.to_datetime(start_time, unit="ns"), pd.to_datetime(end_time, unit="ns")))
    
    if store_slice["ping_time"].size == 0:
        del store
        del store_slice
        raise ValueError(f"No data available between {start_time} and {end_time}")
    
    store_slice = store_slice.sortby('ping_time')

    try:
        # Group by ping_time and take the mean to handle overlaps
        store_slice = store_slice.groupby('ping_time').mean()
    except Exception as e:
        print('Failed to group the data')
    
    del store
    
    return store_slice