from typing import Any, Dict, List, Optional
from prefect import flow

from .tasks import data_convert, get_client, parse_raw_json
from ...settings.models import RawConfig


@flow
def conversion_pipeline(
    config: RawConfig,
    raw_dicts: List[Dict[str, Any]] = [],
    raw_json: Optional[str] = None,
    raw_json_storage_options: Dict[str, Any] = {},
    client=None,
):
    """
    Conversion pipeline for raw echosounder data.
    The results will be converted as weekly files.

    Parameters
    ----------
    config : dict
        Pipeline configuration file
    raw_dicts : list
        List of raw url dictionary
    raw_json : str, optional
        The path to raw urls json file
    raw_json_storage_options: dict
        Storage options for reading raw urls file path
    client : dask.distributed.Client, optional
        The dask client to use for `echopype.combine_echodata`

    Returns
    -------
    List of path string to converted files

    Notes
    -----
    Don't run this pipeline with Dask Task Runners
    """
    all_weeks = parse_raw_json(
        raw_dicts=raw_dicts,
        raw_url_file=raw_json,
        json_storage_options=raw_json_storage_options,
    )
    futures = []
    client = get_client(client)
    for raw_dicts in all_weeks:
        future = data_convert.submit(
            raw_dicts=raw_dicts,
            client=client,
            config=config,
        )
        futures.append(future)
    return [f.result() for f in futures]
