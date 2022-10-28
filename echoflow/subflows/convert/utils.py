import os
from pathlib import Path

import echopype as ep
import requests


def make_temp_folder():
    temp_raw_dir = Path("temp_echopype_output/raw_temp_files")
    temp_raw_dir.mkdir(exist_ok=True, parents=True)
    return temp_raw_dir


def download_temp_file(raw, temp_raw_dir):
    """
    Download temporary raw file
    """
    urlpath = raw.get("file_path")
    req = requests.get(urlpath)
    fname = os.path.basename(urlpath)
    out_path = temp_raw_dir / fname
    with open(out_path, mode="wb") as f:
        f.write(req.content)
    raw.update({"local_path": out_path})
    return raw


def open_and_save(raw):
    """
    Open, convert, and save raw file
    """
    local_file = Path(raw.get("local_path"))
    out_zarr = local_file.parent / local_file.name.replace(".raw", ".zarr")
    ed = ep.open_raw(
        raw_file=local_file,
        sonar_model=raw.get("instrument"),
        offload_to_zarr=True,
    )
    ed.to_zarr(str(out_zarr), overwrite=True)

    # Delete temp zarr
    del ed
    # Delete temp raw
    local_file.unlink()

    return ep.open_converted(str(out_zarr))


def combine_data(ed_list, zarr_path, client):
    combined_ed = ep.combine_echodata(list(ed_list), zarr_path, overwrite=True, client=client)

    # Clean up objects
    del combined_ed
    for ed in ed_list:
        del ed
    return zarr_path
