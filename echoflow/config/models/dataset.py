from typing import Any, Dict, Optional
from pydantic import BaseModel
import jinja2


class Parameters(BaseModel):
    ship_name: str
    survey_name: str
    sonar_model: str


class StorageOptions(BaseModel):
    anon: Optional[bool] = True


class Transect(BaseModel):
    file: str
    storage_options: Dict[str, Any] = {}


class Args(BaseModel):
    urlpath: str
    parameters: Parameters
    storage_options: StorageOptions
    transect: Optional[Transect]
    zarr_store: Optional[str]

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
    storage_options: Optional[StorageOptions] = {}


class Dataset(BaseModel):
    name: str
    sonar_model: str
    raw_regex: str
    args: Args
    output: Output
    passing_params : Optional[Dict[str,Any]]




# Usage example
json_data = {
    "name": "Bell_M._Shimada-SH1707-EK60",
    "sonar_model": "EK60",
    "raw_regex": "someregex",
    "args": {
        "urlpath": "s3://ncei-wcsd-archive/data/raw/{{ ship_name }}/{{ survey_name }}/{{ sonar_model }}/*.raw",
        "parameters": {
            "ship_name": "Bell_M._Shimada",
            "survey_name": "SH1707",
            "sonar_model": "EK60"
        },
        "storage_options": {
            "anon": True
        },
        "transect": {
            "file": "./hake_transects_2017.zip"
        }
    },
    "output": {
        "urlpath": "./combined_files",
        "overwrite": True
    }
}

schema = Dataset(**json_data)
