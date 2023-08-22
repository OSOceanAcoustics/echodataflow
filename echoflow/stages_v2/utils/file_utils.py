

import os
from pathlib import Path
from typing import Any, Dict, List, Tuple, Union
from urllib.parse import urlparse
from echopype import open_converted

import fsspec
from echoflow.config.models.datastore import Dataset
from echoflow.config.models.output_model import Output
from echoflow.config.models.pipeline import Stage
from dateutil import parser
from prefect import task
from fsspec.implementations.local import LocalFileSystem


def download_temp_file(raw, working_dir: str, stage: Stage, config: Dataset):
    """
    Download temporary raw file
    """

    urlpath = raw.get("file_path")
    fname = os.path.basename(urlpath)
    out_path = working_dir+"/"+fname
    working_dir_fs = extract_fs(out_path, storage_options=config.output.storage_options_dict)
    
    if stage.options.get("use_raw_offline") == False or isFile(out_path, config.output.storage_options_dict) == False:
        
        print("Downloading ...", out_path)
        file_system = extract_fs(urlpath, storage_options=config.args.storage_options_dict)
        with file_system.open(urlpath, 'rb') as source_file:
            with working_dir_fs.open(out_path, mode="wb") as f:
                f.write(source_file.read())
    raw.update({"local_path": out_path})
    return raw

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

def make_temp_folder(folder_name:str, storage_options: Dict[str, Any]) -> str:
    """
    Make temporary echopype folder locally at
    `./temp_echopype_output/raw_temp_files`
    """ 
  
    fsmap = fsspec.get_mapper(folder_name, **storage_options)
    if fsmap.fs.isdir(fsmap.root) == False:
        fsmap.fs.mkdir(fsmap.root, exists_ok=True, create_parents=True)
    if isinstance(fsmap.fs, LocalFileSystem):
        return str(Path(folder_name).resolve())
    return folder_name

@task
def get_output_file_path(raw_dicts, config:Dataset):
    """
    Get the output file path based on the raw file dictionary
    datetime key. It will grab the first file and use that as the
    file name in a form of 'DYYYYmmdd-THHMMSS.zarr'.

    This method will also grab value from the config.output.urlpath
    for where the file should go.

    Parameters
    ----------
    raw_dicts : list
        The list of raw url dictionary. Must have 'datetime' key!
    config : RawConfig
        The pydantic RawConfig object that contains all the configurations
        needed.

    Returns
    -------
    The full path string to the zarr file.
    """
    if len(raw_dicts) < 1:
        raise ValueError("There must be at least one raw file dictionary!")

    first_file = raw_dicts[0]
    datetime_obj = parser.parse(first_file.get("datetime"))
    if config.args.transect is None:
        # Only use datetime
        out_fname = datetime_obj.strftime("D%Y%m%d-T%H%M%S.zarr")
    else:
        # Use transect number
        transect_num = first_file.get("transect_num", None)
        date_name = datetime_obj.strftime("D%Y%m%d-T%H%M%S")
        out_fname = f"x{transect_num:04}-{date_name}.zarr"
    return "/".join([config.output.urlpath, out_fname])

def isFile(file_path: str, storage_options: Dict[str, Any]= {}):
    fs = extract_fs(file_path, storage_options=storage_options)
    if file_path.endswith(".zarr"):
        return fs.isdir(file_path)
    return fs.isfile(file_path)


def get_working_dir(stage: Stage, config: Dataset):

    if stage.options is not None and stage.options.get("out_path") is not None:
        working_dir = make_temp_folder(stage.options.get("out_path"), config.output.storage_options_dict)
    elif config.output.urlpath is not None:
        working_dir = make_temp_folder(config.output.urlpath+"/"+stage.name, config.output.storage_options_dict)
    else:
        working_dir = make_temp_folder("Echoflow_working_dir"+"/"+stage.name, config.output.storage_options_dict)
    
    return working_dir

@task
def get_ed_list(config: Dataset, stage: Stage, transect_data: Union[Output, List[Dict]]):
    ed_list = []
    if type(transect_data) == list:
        for zarr_path_data in transect_data:
            ed = open_converted(
                converted_raw_path=str(zarr_path_data.get("out_path")),
                storage_options=dict(config.output.storage_options_dict),
            )
            ed_list.append(ed)
    else:
        zarr_path_data = transect_data.data
        ed = open_converted(
            converted_raw_path=str(zarr_path_data.get("out_path")),
            storage_options=dict(config.output.storage_options_dict),
        )
        ed_list.append(ed)
    return ed_list