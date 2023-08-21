from enum import Enum
from typing import Any, Dict, Optional
from pydantic import BaseModel
import jinja2


class StorageType(Enum):
    AWS = "AWS"
    AZ = "AZ"
    GCP = "GCP"


class StorageOptions(BaseModel):
    type: Optional[StorageType]
    block_name: Optional[str]
    anon: Optional[bool] = False


class Parameters(BaseModel):
    ship_name: str
    survey_name: str
    sonar_model: str


class Transect(BaseModel):
    file: Optional[str]
    storage_options: Optional[StorageOptions] = None
    storage_options_dict: Optional[Dict[str, Any]] = {}  
    default_transect_num: int = None


class Args(BaseModel):
    urlpath: str
    parameters: Parameters
    storage_options: Optional[StorageOptions] = None
    storage_options_dict: Optional[Dict[str, Any]] = {}
    transect: Optional[Transect]
    zarr_store: Optional[str]
    json_export: Optional[bool] = False
    raw_json_path: Optional[str] = None

    @property
    def rendered_path(self):
        """Rendered url path from inputs of parameters"""
        if self.parameters is not None:
            env = jinja2.Environment()
            template = env.from_string(self.urlpath)
            return template.render(self.parameters)
        return self.urlpath


class Output(BaseModel):
    urlpath: str
    overwrite: Optional[bool] = True
    storage_options: Optional[StorageOptions] = None
    storage_options_dict: Optional[Dict[str, Any]] = {}


class Dataset(BaseModel):
    name: str
    sonar_model: str
    raw_regex: str
    args: Args
    output: Output
    passing_params: Optional[Dict[str, Any]]
