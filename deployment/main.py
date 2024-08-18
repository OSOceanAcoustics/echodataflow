import logging
from echodataflow import echodataflow_start
from prefect import flow
from prefect.blocks.core import Block
from typing import Any, Dict, Optional, Union
from pathlib import Path

logging.basicConfig(filename='/home/exouser/Desktop/Echodataflow/echodataflow/logs/service.log', level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(message)s')

@flow
def echodataflow(dataset_config: Union[Dict[str, Any], str, Path],
    pipeline_config: Union[Dict[str, Any], str, Path],
    logging_config: Union[Dict[str, Any], str, Path] = None,
    storage_options: Union[Dict[str, Any], Block] = None,
    options: Optional[Dict[str, Any]] = {},
    json_data_path: Union[str, Path] = None):
    
    echodataflow_start(dataset_config=dataset_config, pipeline_config=pipeline_config, logging_config=logging_config
                       , storage_options=storage_options, options=options, json_data_path=json_data_path)
    
if __name__ == "__main__":
    logging.debug("Starting the Echodataflow service")
    try:
        echodataflow.serve(name="EDF-Echoshader")
        logging.debug("Echodataflow service started successfully")
    except Exception as e:
        logging.error(f"Error starting the Echodataflow service: {e}")
        raise