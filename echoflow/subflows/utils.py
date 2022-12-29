from typing import Any, Dict, Union, Tuple
from urllib.parse import urlparse

import fsspec


def glob_url(path, **storage_options):
    """
    Glob files based on given path string,
    using fsspec.filesystem.glob

    Parameters
    ----------
    path : str
        The url path string glob pattern
    **storage_options : dict, optional
        Extra arguments to pass to the filesystem class,
        to allow permission to glob

    Returns
    -------
    List of paths from glob
    """
    file_system, scheme = extract_fs(
        path, storage_options, include_scheme=True
    )
    all_files = [
        f if f.startswith(scheme) else f"{scheme}://{f}"
        for f in file_system.glob(path)
    ]

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
