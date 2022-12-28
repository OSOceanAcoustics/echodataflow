import re
from typing import Set

PARAM_PATTERN = r"({{[^\n\r/]+}})+"


def get_glob_parameters(glob_str: str) -> Set[str]:
    """
    Extract the variables from glob pattern string based on jinja2
    templating

    Parameters
    ----------
    glob_str : str
        Glob string to be extracted parameters from
        {{ test }} results in {'test'}
    Returns
    -------
    Set
        Set of parameters string
    """
    param_values = re.findall(PARAM_PATTERN, glob_str)
    return {pstr.strip("{{").strip("}}").strip(" ") for pstr in param_values}
