

import os
from pathlib import Path
from echoflow.config.models.dataset import Dataset
from echoflow.config.models.pipeline import Stage
from echoflow.stages_v2.utils.config_utils import extract_fs
from dateutil import parser
from prefect import task


def download_temp_file(raw, temp_raw_dir, stage: Stage):
    """
    Download temporary raw file
    """

    urlpath = raw.get("file_path")
    fname = os.path.basename(urlpath)

    out_path = temp_raw_dir / fname

    if stage.options.get("use_raw_offline") == False or os.path.isfile(out_path) == False:
        print("Downloading ...", out_path)
        file_system = extract_fs(urlpath)
        with file_system.open(urlpath, 'rb') as source_file:
            with open(out_path, mode="wb") as f:
                f.write(source_file.read())

    raw.update({"local_path": out_path})
    return raw


def make_temp_folder(folder_name:str):
    """
    Make temporary echopype folder locally at
    `./temp_echopype_output/raw_temp_files`
    """
    temp_raw_dir = Path(folder_name)
    temp_raw_dir.mkdir(exist_ok=True, parents=True)
    os.chmod(temp_raw_dir, 0o777)
    return temp_raw_dir

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
