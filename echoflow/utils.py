import re

PARAM_PATTERN = r"({{[^\n\r/]+}})+"


def get_glob_parameters(glob_str):
    """
    Extract the variables from glob pattern string based on jinja2
    templating
    """
    param_values = re.findall(PARAM_PATTERN, glob_str)
    return {pstr.strip("{{").strip("}}").strip(" ") for pstr in param_values}
