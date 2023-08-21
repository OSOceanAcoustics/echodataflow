import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Union
from echoflow.stages_v2.aspects.echoflow_aspect import echoflow
from prefect import flow, task
from echoflow.config.models.datastore import Dataset
from echoflow.config.models.output_model import Output
from echoflow.config.models.pipeline import Stage
from echoflow.stages_v2.aspects.echoflow_aspect import echoflow
from echoflow.stages_v2.utils.file_utils import download_temp_file, make_temp_folder
import echopype as ep


@flow(name="DemoFlow")
@echoflow()
def demoFlow(dataset, stage, data):
    # logger = logging.getLogger("echoflow")
    print("I do nothing")
    # logger.log(
    #     msg=f"Exiting with memory at ",
    #     extra={"mod_name": "timepass", "func_name": __name__},
    #     level=logging.DEBUG,
    # )
    # logger.log(
    #     msg=f"Exiting with memory at ",
    #     extra={"mod_name": "timepass", "func_name": __name__},
    #     level=logging.ERROR,
    # )
    # logger.log(
    #     msg=f"Exiting with memory at ",
    #     extra={"mod_name": "timepass", "func_name": __name__},
    #     level=logging.INFO,
    # )
    # logger.log(
    #     msg=f"Exiting with memory at ",
    #     extra={"mod_name": "timepass", "func_name": __name__},
    #     level=logging.WARNING,
    # )

    for i in range(0, 5):
        demoTask.submit()


@task(name="DemoTask")
def demoTask():
    print("I also do nothing btw")
    return 1
