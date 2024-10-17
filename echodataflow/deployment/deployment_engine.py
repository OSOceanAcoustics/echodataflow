from datetime import timedelta
from pathlib import Path
from typing import Optional, Union

from prefect.client.schemas.actions import DeploymentScheduleCreate
from prefect.client.schemas.schedules import IntervalSchedule
from prefect.deployments.runner import RunnerDeployment

from echodataflow.deployment.service import edf_service
from echodataflow.models.deployment.deployment import Deployment, Service
from echodataflow.utils.config_utils import parse_yaml_config
from echodataflow.utils.filesystem_utils import handle_storage_options
from echodataflow.utils.prefect_utils import create_work_pool_and_queue


async def deploy_echodataflow(
    deployment_yaml: Union[dict, str, Path],
    logging_yaml: Optional[Union[dict, str, Path]] = None,
    storage_options: Optional[dict] = None
) -> Deployment:
    storage_options = handle_storage_options(storage_options)
    deployment_dict = parse_yaml_config(deployment_yaml, storage_options=storage_options)
    logging_dict = parse_yaml_config(logging_yaml, storage_options=storage_options) if logging_yaml else None

    default_logging = None
    if logging_dict:
        default_logging = logging_dict.get('default', None)
    
    deployment = Deployment(**deployment_dict)
    
    for service in deployment.services:
        log_dict = None
            
        if service.logging is not None and logging_dict is not None:
            log_dict = logging_dict.get(service.logging.handler, default_logging)
        else:
            log_dict = default_logging
        
        await _deploy_service(service, log_dict)
    
    return deployment

async def _deploy_service(
    service: Service,
    logging_dict: Optional[dict] = None
):
    if not service.name:
        raise ValueError("Service name must be specified!")

    edf_service_fn = edf_service.with_options(name=service.name, description=service.description)
    
    schedule = [DeploymentScheduleCreate(schedule=IntervalSchedule(interval=timedelta(minutes=service.schedule.interval_mins), anchor_date=service.schedule.anchor_date))]
    
    await create_work_pool_and_queue(service.workpool)
        
    deployment: RunnerDeployment = await edf_service_fn.to_deployment(        
        name=service.name,
        parameters={"stages": service.stages, "edf_logger": logging_dict},
        work_queue_name=service.workpool.name,
        work_pool_name=service.workpool.workqueue.name,
        tags=service.tags,
        schedules=schedule        
    )
    
    uuid = await deployment.apply()
    
    print(f"Successfully deployed service: {service.name} with deployment name: {uuid}")