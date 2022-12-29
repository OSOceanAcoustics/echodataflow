from typing import Any, Dict, Union, List, Literal

from prefect import flow

from ..subflows.calibrate import calibrate_pipeline
from .utils import init_configuration


@flow(name="01-open-and-calibrate")
def open_and_calibrate(
    config: Union[Dict[str, Any], str],
    file_paths: List[str] = [],
    storage_options: Dict[str, Any] = {},
    cal_type: Literal["Sv", "TS"] = "Sv",
    open_kwargs: Dict[str, Any] = {},
):
    """
    Level 1 processing for reading converted raw file and perform calibration

    Parameters
    ----------
    config : dict or str
        Path string to configuration file or dictionary
        containing configuration values
    file_paths : list
        List of file paths to converted raw files to be calibrated
    storage_options : dict
        Storage options for reading the converted raw file(s)
    cal_type : {"Sv", "TS"}, optional
        Calibration type to use. Default is "Sv"
    open_kwargs : dict
        Keyword arguments to pass into xarray's `open_dataset` function
    """
    new_config = init_configuration(
        config=config, storage_options=storage_options
    )

    return calibrate_pipeline(
        config=new_config,
        file_paths=file_paths,
        cal_type=cal_type,
        open_kwargs=open_kwargs,
    )
