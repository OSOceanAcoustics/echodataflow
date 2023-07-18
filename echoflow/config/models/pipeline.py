from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

class Stage(BaseModel):
    name: str
    module: Optional[str]
    external_params: Optional[Dict[str, Any]]
    options: Optional[Dict[str, Any]]
    prefect_config: Optional[Dict[str, Any]]

class Pipeline(BaseModel):
    recipe_name: str
    generate_grpah: Optional[bool] = False
    stages: List[Stage]

class Recipe(BaseModel):
    active_recipe: str
    use_previous_recipe: Optional[bool] = False
    database_path: Optional[str] = ''
    pipeline: List[Pipeline]

