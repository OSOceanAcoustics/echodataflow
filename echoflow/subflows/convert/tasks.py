import itertools as it
import json
from typing import Any, Dict, List, Optional

from dask import delayed
from dask.distributed import Client
from prefect import task

from ..utils import extract_fs
from .utils import (
    combine_data,
    download_temp_file,
    make_temp_folder,
    open_and_save,
    get_output_file_path,
)
from ...settings.models import RawConfig


@task
def get_client(client=None):
    if client is None:
        return Client()
    return client


@task
def data_convert(
    raw_dicts: List[Dict[str, Any]],
    config: RawConfig,
    client,
):
    """
    Task for running the data conversion on a list of raw url
    dictionaries.

    Parameters
    ----------
    raw_dicts : list
        The list of raw url dictionary
    config : dict
        Pipeline configuration file
    client : dask.distributed.Client, optional
        The dask client to use for `echopype.combine_echodata`

    Returns
    -------
    String path to the combined echodata file
    """
    zarr_path = get_output_file_path(raw_dicts=raw_dicts, config=config)

    # TODO: Allow for specifying output path
    temp_raw_dir = make_temp_folder()
    ed_tasks = []
    for raw in raw_dicts:
        raw = delayed(download_temp_file)(raw, temp_raw_dir=temp_raw_dir)
        ed = delayed(open_and_save)(raw, config=config)
        ed_tasks.append(ed)
    ed_futures = client.compute(ed_tasks)
    ed_list = client.gather(ed_futures)
    return combine_data(ed_list, zarr_path, client, config)


@task
def parse_raw_json(
    raw_dicts: List[Dict[str, Any]] = [],
    raw_url_file: Optional[str] = None,
    json_storage_options: Dict[Any, Any] = {},
) -> List[List[Dict[str, Any]]]:
    """
    Task to parse raw urls json files and splits them into
    weekly list by utilizing julian days.

    This assumes the following raw url dictionary

    ```
    {'instrument': 'EK60',
    'file_path': 'https://example.com/some-file.raw',
    'month': 1,
    'year': 2017,
    'jday': 1,
    'datetime': '2017-01-01T00:00:00'}
    ```

    Parameters
    ----------
    raw_dicts : list, optional
        List of raw url dictionary
    raw_url_file : str, optional
        Raw urls file path string
    json_storage_options : dict
        Storage options for reading raw urls file path

    Returns
    -------
    List of list of raw urls string,
    broken up to 7 julian days each chunk
    """
    if len(raw_dicts) == 0:
        if raw_url_file is None:
            raise ValueError("Must have raw_dicts or raw_url_file present.")
        file_system = extract_fs(
            raw_url_file, storage_options=json_storage_options
        )
        with file_system.open(raw_url_file) as f:
            raw_dicts = json.load(f)

    # Number of days for a week chunk
    n = 7

    all_jdays = sorted({r.get("jday") for r in raw_dicts})
    split_days = [
        all_jdays[i : i + n] for i in range(0, len(all_jdays), n)
    ]  # noqa

    day_dict = {}
    for r in raw_dicts:
        mint = r.get("jday")
        if mint not in day_dict:
            day_dict[mint] = []
        day_dict[mint].append(r)

    all_weeks = []
    for week in split_days:
        files = list(it.chain.from_iterable([day_dict[d] for d in week]))
        all_weeks.append(files)

    return all_weeks
