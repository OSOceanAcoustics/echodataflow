from typing import Dict, List, Optional
from pydantic import BaseModel, Field, field_validator

class DeploymentSchedule(BaseModel):
    anchor_date: str = Field(..., description="The start date from which the schedule will compute its intervals.")
    interval: int = Field(..., gt=0, description="The number of minutes between each scheduled run.")
    timezone: str = Field(..., description="Timezone for the schedule, e.g., 'UTC', 'America/New_York'.")
    cron: str = Field(..., description="Cron expression for more complex scheduling.")

    @field_validator('cron')
    def validate_cron(cls, value):
        if not value.startswith('*/'):
            raise ValueError("Invalid cron format")
        return value

class WorkPool(BaseModel):
    name: str = Field(..., description="Name of the workpool.")    
    work_queue_name: Optional[str] = Field('default', description="Name of the work queue to use.")
    job_variables: Dict = Field(default_factory=dict, description="Optional variables for jobs in the workpool.")

class Service(BaseModel):
    name: str = Field(..., description="Name of the service.")
    module: str = Field(..., description="Python module containing the service definitions.")
    schedule: Optional[DeploymentSchedule] = Field(None, description="Scheduling details for the service.")
    tags: List[str] = Field(default_factory=list, description="List of tags associated with the service.")
    description: Optional[str] = Field(None, description="Description of the service.")
    version: Optional[str] = Field(None, description="Version of the service.")
    entrypoint: Optional[str] = Field(None, description="Entrypoint command or script for the service.")
    parameters: Dict = Field(default_factory=dict, description="Parameters for the service.")
    workpool: Optional[WorkPool] = Field(None, description="WorkPool configuration for the service.")

class Deployment(BaseModel):
    services: List[Service] = Field(..., description="List of services included in the deployment.")
