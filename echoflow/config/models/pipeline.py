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
from pydantic import BaseModel, Field

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
        generate_graph (Optional[bool]): Flag to indicate whether to generate a graph. Default is False.
        stages (List[Stage]): List of stages in the pipeline.
    """
    recipe_name: str
    generate_graph: Optional[bool] = False
    stages: List[Stage]

class Recipe(BaseModel):
    """
    Model for representing a recipe.

    Attributes:
        active_recipe (str): The active recipe name.
        use_local_dask (bool): Flag to indicate whether to use local Dask. Default is False.
        scheduler_address (str): The scheduler address. Default is None.
        use_previous_recipe (Optional[bool]): Flag to indicate whether to use a previous recipe. Default is False.
        database_path (Optional[str]): The path to the database. Default is an empty string.
        pipeline (List[Pipeline]): List of pipelines in the recipe.
    """
    active_recipe: str
    use_local_dask: bool = False
    scheduler_address: str = None
    use_previous_recipe: Optional[bool] = False
    database_path: Optional[str] = ''
    pipeline: List[Pipeline]

    
