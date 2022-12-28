from typing import Any, Dict, Optional

from prefect import flow

from ..subflows.convert import conversion_pipeline
from ..subflows.fetch import find_raw_pipeline


@flow(name="00-fetch-and-convert")
def fetch_and_convert(
    config: Dict[Any, Any],
    parameters: Dict[Any, Any] = {},
    export: bool = False,
    export_path: str = "",
    export_storage_options: Dict[Any, Any] = {},
):
    """
    Level 0 processing for fetching and converting raw sonar data

    Parameters
    ----------
    config : dict
        Pipeline configuration file
    parameters : dict
        Input parameters for creating the full url path.
        *These inputs will overwrite the parameters,
        within the config.*
    export : bool
        Flag to export raw paths for individual raw files
        to a raw urls JSON file
    export_path : str
        Path to store raw urls JSON file if export is True
    export_storage_options: dict
        Storage options for destination to store paths file

    Notes
    -----
    DO NOT use Dask Task Runner for this flow.
    Dask is used underneath within echopype.
    """
    raw_dicts, new_config = find_raw_pipeline(
        config=config,
        parameters=parameters,
        export=export,
        export_path=export_path,
        export_storage_options=export_storage_options,
    )

    return conversion_pipeline(
        config=new_config,
        raw_dicts=raw_dicts,
        raw_json=export_path,
        raw_json_storage_options=export_storage_options,
    )
