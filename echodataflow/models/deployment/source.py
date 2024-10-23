from typing import Optional, Dict, Any, Union, List
import jinja2
from pydantic import BaseModel, Field

from echodataflow.models.deployment.storage_options import StorageOptions

class Parameters(BaseModel):
    """
    Model for defining parameters.

    Attributes:
        ship_name (Optional[str]): The name of the ship.
        survey_name (Optional[str]): The name of the survey.
        sonar_model (Optional[str]): The model of the sonar.
        file_name (Optional[Union[List[str], str]]): The name of the file(s).
    """
    ship_name: Optional[str] = Field(None, description="The name of the ship conducting the survey.")
    survey_name: Optional[str] = Field(None, description="The name of the survey or project.")
    sonar_model: Optional[str] = Field(None, description="The sonar model used for data acquisition.")
    file_name: Optional[Union[List[str], str]] = Field(None, description="Name of the file or a list of file names.")

class Source(BaseModel):
    """
    Model for defining the source of the data.

    Attributes:
        path (Optional[Union[str, Dict[str, List[str]]]]): URL path or path pattern for the source data.
        parameters (Optional[Parameters]): Parameters to apply to the source path.
        window_options (Optional[Dict[str, Any]]): Time window options for slicing the source data.
        storage_options (Optional[StorageOptions]): Storage options for accessing the source data.
    """
    path: Union[str, Dict[str, List[str]]] = Field(..., description="Source URL path or folder structure of the data.")
    parameters: Optional[Parameters] = Field(None, description="Parameters to apply to the source.")
    window_options: Optional[Dict[str, Any]] = Field(None, description="Window options for the source.")
    storage_options: Optional[StorageOptions] = Field(None, description="Storage options for the source.")
    raw_regex: Optional[str] = Field("(.*)-?D(?P<date>\w{1,8})-T(?P<time>\w{1,6})", description="Raw regex pattern for the source.")

    def render_path(self) -> Union[str, Dict[str, List[str]]]:
        """
        Render the URL path using the provided parameters.

        Returns:
            Union[str, Dict[str, List[str]]]: Rendered URL path or a dictionary of rendered paths.
        """
        # Initialize a Jinja environment
        env = jinja2.Environment()

        # If `urlpath` is a string, render it with parameters
        if isinstance(self.path, str):
            return self._render_template(self.path, env)

        # If `urlpath` is a dictionary, render each value in the dictionary
        elif isinstance(self.path, dict):
            rendered_dict = {}
            for key, value in self.path.items():
                # Assume value is a list of strings that need rendering
                rendered_list = [self._render_template(v, env) for v in value]
                rendered_dict[key] = rendered_list
            return rendered_dict

        return self.path

    def _render_template(self, template_str: str, env: jinja2.Environment) -> str:
        """
        Render a single template string using Jinja2.

        Args:
            template_str (str): Template string to be rendered.
            env (jinja2.Environment): Jinja2 environment for rendering.

        Returns:
            str: Rendered template string.
        """
        template = env.from_string(template_str)
        return template.render(self.parameters.dict() if self.parameters else {})
    
    def extract_source(self):
        
        from echodataflow.utils.config_utils import glob_all_files
        
        if self.window_options is not None:
            
            # Treat source as a folder and iterate over files to collect and group relevant files
            pass
        else:
            
            total_files = glob_all_files(config=self)
            
            return total_files
    
    
    class Config:
        # Allow arbitrary field types and definitions in nested dictionaries
        arbitrary_types_allowed = True
        
