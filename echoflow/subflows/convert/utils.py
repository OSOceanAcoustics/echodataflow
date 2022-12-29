import os
from pathlib import Path

import echopype as ep
from dateutil import parser

from echoflow.settings.models.raw_config import RawConfig

from ..utils import extract_fs

ep.verbose()


def make_temp_folder():
    """
    Make temporary echopype folder locally at
    `./temp_echopype_output/raw_temp_files`
    """
    temp_raw_dir = Path("temp_echopype_output/raw_temp_files")
    temp_raw_dir.mkdir(exist_ok=True, parents=True)
    return temp_raw_dir


def get_output_file_path(raw_dicts, config):
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


def download_temp_file(raw, temp_raw_dir):
    """
    Download temporary raw file
    """
    urlpath = raw.get("file_path")
    file_system = extract_fs(urlpath)
    fname = os.path.basename(urlpath)
    out_path = temp_raw_dir / fname
    with file_system.open(urlpath, 'rb') as source_file:
        with open(out_path, mode="wb") as f:
            f.write(source_file.read())
    raw.update({"local_path": out_path})
    return raw


def open_and_save(raw, config: RawConfig, **kwargs):
    """
    Open, convert, and save raw file
    """
    local_file = Path(raw.get("local_path"))
    out_zarr = "/".join(
        [config.output.urlpath, local_file.name.replace(".raw", ".zarr")]
    )
    ed = ep.open_raw(
        raw_file=local_file,
        sonar_model=raw.get("instrument"),
        offload_to_zarr=True,
        **kwargs
    )
    ed.to_zarr(
        save_path=str(out_zarr),
        overwrite=True,
        output_storage_options=config.output.storage_options,
    )

    # Delete temp zarr
    del ed
    # Delete temp raw
    local_file.unlink()

    return ep.open_converted(
        converted_raw_path=str(out_zarr),
        storage_options=config.output.storage_options,
    )


def clean_up_files(ed_list, config):
    """Clean up converted raw files"""
    for ed in ed_list:
        path = str(ed.converted_raw_path)
        file_system = extract_fs(
            path, storage_options=config.output.storage_options
        )
        file_system.delete(path, recursive=True)
        del ed


def combine_data(ed_list, zarr_path, client, config: RawConfig, **kwargs):
    """Combine echodata object and clean up the individual files"""
    combined_ed = ep.combine_echodata(
        echodatas=list(ed_list),
        zarr_path=zarr_path,
        client=client,
        overwrite=config.output.overwrite,
        storage_options=config.output.storage_options,
        **kwargs
    )

    # Clean up objects
    del combined_ed
    clean_up_files(ed_list, config)
    return zarr_path
