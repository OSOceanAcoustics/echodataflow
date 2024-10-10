from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field

# Import models assuming they are defined in the respective module paths
from echodataflow.models.deployment.data_quality import DataQuality
from echodataflow.models.deployment.source import Source
from echodataflow.models.deployment.storage_options import StorageOptions


# Define additional models required for Stage
class Destination(BaseModel):
    """
    Model for defining the destination of the data.
    
    Attributes:
        path (Optional[str]): The path where the data should be stored.
        storage_options (Optional[StorageOptions]): Storage options for the destination path.
    """
    path: Optional[str] = Field(None, description="Destination path of the data.")
    storage_options: Optional[StorageOptions] = Field(None, description="Storage options for the destination.")


class Group(BaseModel):
    """
    Model for defining the grouping of data in the pipeline.

    Attributes:
        file (Optional[str]): The file path used for grouping operations.
        grouping_regex (Optional[str]): Regex pattern for grouping files based on filenames.
        storage_options (Optional[StorageOptions]): Storage options for grouping operations.
    """
    file: Optional[str] = Field(None, description="File path for grouping operations.")
    grouping_regex: Optional[str] = Field(None, description="Regex pattern for grouping files based on filename.")
    storage_options: Optional[StorageOptions] = Field(None, description="Storage options for grouping.")


class Task(BaseModel):
    """
    Model for defining a task in the data pipeline.

    Attributes:
        name (str): Name of the task. This is a required field.
        module (Optional[str]): Python module containing the task definition.
        task_params (Optional[Dict[str, Any]]): Additional parameters for configuring the task.
    """
    name: str = Field(..., description="Name of the task.")
    module: str = Field(..., description="Python module containing the task definition.")
    task_params: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Parameters for the task.")


class Stage(BaseModel):
    """
    Model for defining a stage in a data pipeline.

    Attributes:
        name (str): Name of the stage.
        module (str): Python module containing the service definitions.
        stage_params (Optional[Dict[str, Any]]): Parameters for configuring the stage.
        source (Optional[Source]): Source of the data.
        group (Optional[Group]): Grouping of the data.
        destination (Optional[Destination]): Destination of the data.
        data_quality (Optional[DataQuality]): Data quality checks configuration.
        options (Optional[Dict[str, Any]]): Additional options for configuring the stage.
        prefect_config (Optional[Dict[str, Any]]): Prefect configuration for the stage.
        tasks (Optional[List[Task]]): List of tasks to be executed in the stage.
    """
    name: str = Field(..., description="Name of the stage. This is a required field and should be unique.")
    module: str = Field(..., description="Python module containing the service definitions. E.g., 'echodataflow.stages.subflows'.")
    stage_params: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Dictionary of parameters to configure the stage.")
    source: Source = Field(..., description="Source of the data. Must be a valid Source object or None.")
    group: Optional[Group] = Field(None, description="Grouping of the data. Must be a valid Group object or None.")
    destination: Optional[Destination] = Field(None, description="Destination of the data. Must be a valid Destination object or None.")
    data_quality: Optional[DataQuality] = Field(None, description="Data quality checks configuration.")
    options: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Additional options for the stage. Used for stage-specific configuration.")
    prefect_config: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Prefect configuration for managing flow control in the stage.")
    tasks: Optional[List[Task]] = Field(default_factory=list, description="List of tasks to be executed in the stage.")

   