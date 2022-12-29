from typing import List, Literal, Dict, Any

from prefect import flow

from ...settings.models import MainConfig
from .tasks import open_and_calibrate


@flow
def calibrate_pipeline(
    config: MainConfig,
    file_paths: List[str],
    cal_type: Literal["Sv", "TS"] = "Sv",
    open_kwargs: Dict[str, Any] = {},
) -> List[str]:
    """
    Calibration pipeline for converted raw files.

    Parameters
    ----------
    config : object
        Pipeline configuration object
    file_paths : list
        List of file paths to converted raw files to be calibrated
    cal_type : {"Sv", "TS"}, optional
        Calibration type to use. Default is "Sv"
    open_kwargs : dict
        Keyword arguments to pass into xarray's `open_dataset` function

    Returns
    -------
    List of path string to converted files

    Notes
    -----
    Don't run this pipeline with Dask Task Runners
    """
    futures = []
    for urlpath in file_paths:
        future = open_and_calibrate.submit(
            config=config,
            file_path=urlpath,
            cal_type=cal_type,
            open_kwargs=open_kwargs,
        )
        futures.append(future)
    return [f.result() for f in futures]
