from typing import Any, Dict, Union

from echoflow.stages_v2.subflows.initialization_flow import init_flow
from echoflow.stages_v2.utils.config_utils import check_config, extract_config
import logging.config
import yaml

from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner

@flow(name="Pipeline-Trigger", task_runner=SequentialTaskRunner())
def pipeline_trigger(
        dataset_config : Union[Dict[str, Any], str],
        pipeline_config : Union[Dict[str, Any], str],
        logging_config : Union[Dict[str, Any], str]
        ):
    
    # setup_logging(logging_config)
    # logger = logging.getLogger(__name__)
    # logger.debug('These are new logs : Debug message')
    # logger.info('These are new logs : Info message')
    # logger.warning('These are new logs : Warning message')
    # logger.error('These are new logs : Error message')

    if (type(dataset_config) == str):
        if not dataset_config.endswith(('.yaml', '.yml')):
            raise ValueError("Configuration file must be a YAML!")
        dataset_config_dict = extract_config(dataset_config)
    if (type(pipeline_config) == str):
        if not pipeline_config.endswith(('.yaml', '.yml')):
            raise ValueError("Configuration file must be a YAML!")
        pipeline_config_dict = extract_config(pipeline_config)

    # Do any config checks on config dicts
    check_config(dataset_config_dict,pipeline_config_dict)
    
    return init_flow(dataset_config_dict,pipeline_config_dict)

    
@task
def setup_logging(config_file):
    with open(config_file, 'r') as file:
        config = yaml.safe_load(file.read())
        logging.config.dictConfig(config)
