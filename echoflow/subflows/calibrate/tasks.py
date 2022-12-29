from typing import Dict, Any, Literal

from prefect import task

from echopype import open_converted
from echopype.calibrate import compute_Sv, compute_TS
import xarray as xr

from ...settings.models import MainConfig


@task
def open_and_calibrate(
    config: MainConfig,
    file_path: str,
    cal_type: Literal["Sv", "TS"] = "Sv",
    open_kwargs: Dict[str, Any] = {},
) -> xr.Dataset:
    """
    Task for opening converted file, and calibrating

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
    xr.Dataset
        Xarray dataset of the calibrated data
    """
    storage_options = config.output.storage_options
    calibrate_config = (
        config.echopype.calibrate if config.echopype is not None else None
    )
    echodata = open_converted(
        converted_raw_path=file_path,
        storage_options=storage_options,
        **open_kwargs
    )
    if cal_type == "Sv":
        calibrate = compute_Sv
        kwargs = (
            calibrate_config.compute_Sv if calibrate_config is not None else {}
        )
    elif cal_type == "TS":
        calibrate = compute_TS
        kwargs = (
            calibrate_config.compute_TS if calibrate_config is not None else {}
        )
    else:
        raise ValueError("cal_type must be Sv or TS")

    return calibrate(echodata=echodata, **kwargs)
