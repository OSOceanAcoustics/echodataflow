from pathlib import Path
from typing import Optional, Union
from echodataflow.models.deployment.deployment import Deployment, Service
from echodataflow.deployment.service import edf_service
from echodataflow.utils.config_utils import handle_storage_options, parse_yaml_config
from prefect.client.schemas.schedules import IntervalSchedule
from prefect.client.schemas.actions import DeploymentScheduleCreate
from datetime import timedelta


async def deploy_echodataflow(
    deployment_yaml: Union[dict, str, Path],
    logging_yaml: Optional[Union[dict, str, Path]] = None,
    storage_options: Optional[dict] = None
):
    storage_options = handle_storage_options(storage_options)
    deployment_dict = parse_yaml_config(deployment_yaml, storage_options=storage_options)
    logging_dict = parse_yaml_config(logging_yaml, storage_options=storage_options) if logging_yaml else None

    deployment = Deployment(**deployment_dict)

    for service in deployment.services:    
        await _deploy_service(service, logging_dict)

async def _deploy_service(
    service: Service,
    logging_dict: Optional[dict] = None
):
    if not service.name:
        raise ValueError("Service name must be specified!")

    edf_service_fn = edf_service.with_options(name=service.name, description=service.description)
    
    schedule = [DeploymentScheduleCreate(schedule=IntervalSchedule(interval=timedelta(minutes=service.schedule.interval_mins), anchor_date=service.schedule.anchor_date))]
    
    # TODO: Create workpool and workqueue if they don't exist
    
    deployment = await edf_service_fn.to_deployment(        
        name=service.name,
        parameters={"stages": service.stages, "edf_logger": logging_dict},
        work_queue_name=service.workqueue,
        work_pool_name=service.workpool,
        tags=service.tags,
        schedules=schedule        
    )
    
    uuid = await deployment.apply()

    print(f"Successfully deployed service: {service.name} with deployment name: {uuid}")