from pathlib import Path
from typing import Optional, Union

from prefect import Flow

from echodataflow.models.deployment import Deployment, Service
from echodataflow.utils.config_utils import extract_config, handle_storage_options
from echodataflow.utils.function_utils import dynamic_function_call


def deploy_echodataflow(
    deployment_yaml: Union[dict, str, Path],
    pipeline_yaml: Union[dict, str, Path],
    datastore_yaml: Union[dict, str, Path],
    logging_yaml: Optional[Union[dict, str, Path]] = None,
    storage_options: Optional[dict] = None,
):
    
    storage_options = handle_storage_options(storage_options)
        
    if isinstance(deployment_yaml, Path):
        deployment_yaml = str(deployment_yaml)
        
    if isinstance(deployment_yaml, str):
        if not deployment_yaml.endswith((".yaml", ".yml")):
            raise ValueError("Configuration file must be a YAML!")
        deployment_yaml_dict = extract_config(deployment_yaml, storage_options)        
    elif isinstance(deployment_yaml, dict):
        deployment_yaml_dict = deployment_yaml
        
    deployment = Deployment(**deployment_yaml_dict)
    
    for service in deployment.services:        
        _deploy_service(service, pipeline_yaml, datastore_yaml, logging_yaml, storage_options)
        
def _deploy_service(
    service: Service,
    pipeline_yaml: Union[dict, str, Path],
    datastore_yaml: Union[dict, str, Path],
    logging_yaml: Optional[Union[dict, str, Path]] = None,
    storage_options: Optional[dict] = None,
):
    if service.name is None:
        raise ValueError("Service name must be specified!")

    pipeline, datastore, logging = extract_service_details(service.name, pipeline_yaml, datastore_yaml, logging_yaml, storage_options)
    
    flow: Flow = dynamic_function_call(service.module, service.name)
    flow.to_deployment()

        
    pass