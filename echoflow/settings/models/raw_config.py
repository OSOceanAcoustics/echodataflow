from typing import Any, Dict, Optional

import jinja2
from pydantic import BaseModel, validator

from ...utils import get_glob_parameters


class Output(BaseModel):
    urlpath: str
    storage_options: Dict[str, Any] = {}
    overwrite: bool = False


class Args(BaseModel):
    urlpath: str
    parameters: Optional[Dict[str, Any]]
    storage_options: Dict[str, Any] = {}
    # Set internally
    rendered_path: Optional[str]

    class Config:
        validate_assignment = True
        underscore_attrs_are_private = True

    @property
    def rendered_path(self):
        if self.parameters is not None:
            env = jinja2.Environment()
            template = env.from_string(self.urlpath)
            return template.render(**self.parameters)
        return self.urlpath

    @validator("parameters", pre=True, always=True)
    def check_parameters(cls, glob_params, values):
        param_values = get_glob_parameters(values.get("urlpath"))
        if len(param_values) > 0 and glob_params is None:
            raise ValueError(
                "Parameters found in `urlpath` but not defined in `parameters` setting.",  # noqa
            )
        missing_keys = []
        for p in param_values:
            if p not in glob_params:
                missing_keys.append(p)

        if len(missing_keys) > 0:
            raise ValueError(f"{', '.join(missing_keys)} is/are missing from parameters.")
        return glob_params


class RawConfig(BaseModel):
    name: str
    sonar_model: str
    raw_regex: str
    args: Args
    output: Output
