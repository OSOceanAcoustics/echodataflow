import os
from typing import List, Dict, Any, Union
import fsspec
from prefect import flow, task

from echodataflow.utils.config_utils import glob_url


@task
def download_temp_file(file_url: str, storage_options: Dict[str, Any], dest_dir: str) -> str:
    """
    Downloads a file from a URL to a destination directory.

    Args:
        file_url (str): URL of the file to download.
        storage_options (Dict[str, Any]): Dictionary containing storage options for source and destination.
        dest_dir (str): Destination directory where the file will be downloaded.

    Returns:
        str: Local path of the downloaded file.
    """

    fname = os.path.basename(file_url)
    out_path = os.path.join(dest_dir, fname)

    # Ensure destination directory exists
    os.makedirs(dest_dir, exist_ok=True)

    # Extract file systems for source and destination
    file_system_source = fsspec.filesystem("s3", **storage_options.get("source", {}))
    file_system_dest = fsspec.filesystem("file", **storage_options.get("dest", {}))

    # Check if file needs to be downloaded
    if not file_system_dest.exists(out_path):
        print(f"Downloading {file_url} to {out_path} ...")
        with file_system_source.open(file_url, "rb") as source_file:
            with file_system_dest.open(out_path, "wb") as dest_file:
                dest_file.write(source_file.read())

    return out_path


@flow
def edf_data_transfer(
    source: Union[List[str], str],
    destination: str = "./temp",
    source_storage_options: Dict[str, Any] = {},
    destination_storage_options: Dict[str, Any] = {},
):
    """
    Downloads multiple files from a list of URLs to a destination directory.

    Args:
        file_urls (List[str]): List of file URLs to download.
        storage_options (Dict[str, Any]): Dictionary containing storage options for source and destination.
        dest_dir (str): Destination directory where the files will be downloaded.
    """
    downloaded_files = []
    if not source:
        raise ValueError("No Source Provided.")

    if isinstance(source, str):
        files = glob_url(source, source_storage_options)
    else:
        files = source

    storage_options: Dict[str, Any] = {}
    storage_options["source"] = source_storage_options
    storage_options["dest"] = destination_storage_options

    for file_url in files:
        local_path = download_temp_file.submit(file_url, storage_options, destination)
        downloaded_files.append(local_path)

    return downloaded_files


if __name__ == "__main__":
    edf_data_transfer.serve()
