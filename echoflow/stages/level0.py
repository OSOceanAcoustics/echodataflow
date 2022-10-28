from typing import Any, Dict, Optional
from prefect import flow

from ..subflows.fetch import find_raw_pipeline
from ..subflows.convert import conversion_pipeline


@flow(name="00-fetch-and-convert")
def fetch_and_convert(
    config,
    deployment: int,
    parameters: Dict[Any, Any] = {},
    export: bool = False,
    export_path: Optional[str] = None,
    export_storage_options: Dict[Any, Any] = {},
):
    """
    Level 0 processing for fetching and converting raw sonar data

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
        deployment,
        raw_dicts=raw_dicts,
        raw_url_file=export_path,
        client=None,
        config=new_config,
    )
