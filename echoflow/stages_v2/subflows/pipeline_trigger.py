from datetime import datetime
from typing import Any, Dict, Union
from echoflow.config.models.dataset import Dataset
from echoflow.config.models.log_object import Log
from echoflow.config.models.pipeline import Recipe

from echoflow.stages_v2.subflows.initialization_flow import init_flow
from echoflow.stages_v2.utils.config_utils import check_config, extract_config
import logging.config
import yaml

from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner

from echoflow.stages_v2.utils.global_db_logger import get_log, init_global
from echoflow.stages_v2.utils.databse_utils import create_log_table, get_connection, get_last_log, insert_log_data


@flow(name="Pipeline-Trigger", task_runner=SequentialTaskRunner())
def pipeline_trigger(
        dataset_config : Union[Dict[str, Any], str],
        pipeline_config : Union[Dict[str, Any], str],
        logging_config : Union[Dict[str, Any], str]
        ):
    log = Log()
    try:
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
        pipeline = Recipe(**pipeline_config_dict)
        dataset = Dataset(**dataset_config_dict)

        log = setup_echoflow_db(pipeline=pipeline)
        init_global(log)

        return init_flow(dataset=dataset,pipeline=pipeline)
    except Exception as e:
        log = get_log()
        log.status = False
        print(e)
    finally:
        log = get_log()
        log.end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        print(log)
        insert_log_data(path=pipeline.database_path, log=log)
    

    
@task
def setup_logging(config_file):
    with open(config_file, 'r') as file:
        config = yaml.safe_load(file.read())
        logging.config.dictConfig(config)

@task
def setup_echoflow_db(pipeline: Recipe):
    last_log = Log()
    try:
        conn = get_connection(pipeline.database_path)
        create_log_table(conn=conn)
        if(pipeline.use_previous_recipe == True):
            conn = get_connection()
            last_log = get_last_log(conn)
        
        last_log.start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        last_log.run_id = None
        return last_log
    except Exception as e:
        print("Failed to create Database with below error")
        print(e)
    finally:
        if conn:
            conn.close()

