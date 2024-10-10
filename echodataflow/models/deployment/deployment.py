from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, field_validator

# Assuming these imports exist in your module
from echodataflow.models.deployment.deployment_schedule import DeploymentSchedule
from echodataflow.models.deployment.stage import Stage
from echodataflow.models.deployment.storage_options import StorageOptions


class EDFLogging(BaseModel):
    """
    Model for defining logging configuration for a service.

    Attributes:
        handler (str): The name of the logging handler.
        kafka (Optional[Dict[str, Any]]): Kafka configuration details for logging.
    """
    handler: str = Field(..., description="Logging handler name.")
    kafka: Optional[Dict[str, Any]] = Field(None, description="Kafka logging configuration details.")

    @field_validator("handler", mode="before")
    def validate_handler(cls, v):
        if not v or not isinstance(v, str) or v.strip() == "":
            raise ValueError("Logging handler must be a non-empty string.")
        return v


class Service(BaseModel):
    """
    Model for defining a service in the deployment pipeline.

    Attributes:
        name (str): Name of the service.
        tags (Optional[List[str]]): List of tags associated with the service.
        description (Optional[str]): Description of the service.
        schedule (Optional[DeploymentSchedule]): Scheduling details for the service.
        stages (Optional[List[Stage]]): List of stages included in the service.
        logging (Optional[EDFLogging]): Logging configuration for the service.
        workpool (Optional[str]): WorkPool configuration for the service.
        workqueue (Optional[str]): WorkQueue configuration for the service.
    """
    name: str = Field(..., description="Name of the service.")
    tags: Optional[List[str]] = Field(default_factory=list, description="List of tags associated with the service. Tags must be unique.")
    description: Optional[str] = Field(None, description="Description of the service.")
    schedule: Optional[DeploymentSchedule] = Field(None, description="Scheduling details for the service.")
    stages: List[Stage] = Field(None, description="List of stages included in the service.")
    logging: Optional[EDFLogging] = Field(None, description="Logging configuration for the service.")
    workpool: Optional[str] = Field('Echodataflow-Workpool', description="WorkPool configuration for the service.")
    workqueue: Optional[str] = Field('default', description="WorkQueue configuration for the service.")

    # Validators
    @field_validator("name", mode="before")
    def validate_service_name(cls, v):
        if not v or not isinstance(v, str) or v.strip() == "":
            raise ValueError("Service name must be a non-empty string.")
        return v

    @field_validator("tags", mode="before")
    def validate_tags(cls, v):
        if v:
            if not isinstance(v, list):
                raise ValueError("Tags must be a list of strings.")
            # Remove duplicates and ensure all tags are non-empty strings
            unique_tags = set()
            for tag in v:
                if not isinstance(tag, str) or tag.strip() == "":
                    raise ValueError("Each tag must be a non-empty string.")
                unique_tags.add(tag.strip())
            return list(unique_tags)
        return v


class Deployment(BaseModel):
    """
    Model for defining a deployment with multiple services.

    Attributes:
        out_path (Optional[str]): Base path for all services.
        storage_options (Optional[StorageOptions]): Base storage options applied to all paths.
        services (List[Service]): List of services included in the deployment.
    """
    out_path: Optional[str] = Field(None, description="Base path for all services. This path will be used if no specific service path is defined.")
    storage_options: Optional[StorageOptions] = Field(None, description="Base Storage options, applied to all paths.")
    services: List[Service] = Field(..., description="List of services included in the deployment.")

    # Validators
    @field_validator("services", mode="before")
    def validate_services(cls, v):
        if not isinstance(v, list) or not v:
            raise ValueError("Services must be a non-empty list of Service objects.")
        return v
