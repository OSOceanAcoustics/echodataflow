"""
Pipeline Configuration Models for Echoflow

This module defines Pydantic models for representing pipeline configurations in Echoflow.

Classes:
    Stage (BaseModel): Model for representing pipeline stages.
    Pipeline (BaseModel): Model for representing pipelines.
    Recipe (BaseModel): Model for representing recipes.

Author: Soham Butala
Email: sbutala@uw.edu
Date: August 22, 2023
"""
from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class Stage(BaseModel):
    """
    Model for representing pipeline stages.

    Attributes:
        name (str): The name of the stage.
        module (Optional[str]): The module associated with the stage.
        external_params (Optional[Dict[str, Any]]): External parameters for the stage.
        options (Optional[Dict[str, Any]]): Options for the stage.
        prefect_config (Optional[Dict[str, Any]]): Prefect configuration for the stage.
    """
    name: str
    module: Optional[str]
    external_params: Optional[Dict[str, Any]]
    options: Optional[Dict[str, Any]]
    prefect_config: Optional[Dict[str, Any]]

class Pipeline(BaseModel):
    """
    Model for representing a pipeline.

    Attributes:
        recipe_name (str): The name of the recipe.
        stages (List[Stage]): List of stages in the pipeline.
    """
    recipe_name: str
    stages: List[Stage]

class Recipe(BaseModel):
    """
    Model for representing a recipe.

    Attributes:
        active_recipe (str): The active recipe name.
        use_local_dask (bool): Flag to indicate whether to use local Dask. Default is False.
        n_workers (int): Number of workers to spin up for local cluster. Default is 3 
        scheduler_address (str): The scheduler address. Default is None.
        pipeline (List[Pipeline]): List of pipelines in the recipe.
    """
    active_recipe: str
    use_local_dask: bool = False
    n_workers: int = 3
    scheduler_address: str = None
    pipeline: List[Pipeline]

    
