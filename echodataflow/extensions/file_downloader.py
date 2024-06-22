import os
from typing import List, Dict, Any, Union
import fsspec
from prefect import flow, task
from prefect.concurrency.sync import concurrency

from echodataflow.utils.config_utils import glob_url
from echodataflow.utils.file_utils import extract_fs, make_temp_folder


@task
def download_temp_file(file_url: str, storage_options: Dict[str, Any], dest_dir: str, delete_on_transfer: bool) -> str:
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
    make_temp_folder(dest_dir, storage_options.get("dest", {}))

    # Extract file systems for source and destination
    file_system_source = extract_fs(file_url, storage_options.get("source", {}))
    file_system_dest = extract_fs(out_path, storage_options.get("dest", {}))

    # Check if file needs to be downloaded
    if not file_system_dest.exists(out_path):
        with concurrency("edf-data-transfer", occupy=1):
            print(f"Downloading {file_url} to {out_path} ...")
            with file_system_source.open(file_url, "rb") as source_file:
                with file_system_dest.open(out_path, "wb") as dest_file:
                    dest_file.write(source_file.read())
        if delete_on_transfer:
            try:
                file_system_source.rm(file_url)
                print("Cleanup complete")
            except Exception as e:
                print(e)
                print("Failed to cleanup " + file_url)

    return out_path


@flow
def edf_data_transfer(
    source: Union[List[str], str],
    destination: str = "./temp",
    source_storage_options: Dict[str, Any] = {},
    destination_storage_options: Dict[str, Any] = {},
    delete_on_transfer=False
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
        local_path = download_temp_file.submit(file_url, storage_options, destination, delete_on_transfer)
        downloaded_files.append(local_path)

    return downloaded_files


if __name__ == "__main__":
    edf_data_transfer.serve()
