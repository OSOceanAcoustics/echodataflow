from typing import Any, Dict
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
    file_system = extract_fs(path, storage_options)
    return file_system.glob(path)


def extract_fs(url: str, storage_options: Dict[Any, Any] = {}):
    """
    Extract the fsspec file system from a url path.

    Parameters
    ----------
    url : str
        The url path string to be parsed and file system extracted
    storage_options : dict
        Additional keywords to pass to the filesystem class

    Returns
    -------
    Filesystem for given protocol and arguments
    """
    parsed_path = urlparse(url)
    file_system = fsspec.filesystem(parsed_path.scheme, **storage_options)
    return file_system
