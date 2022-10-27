import copy
import itertools as it

from prefect import task
import fsspec

from jinja2 import Environment

from ..utils import glob_url
from .utils import parse_file_path


@task
def setup_config(input_config, **parameters):
    jinja = Environment()
    config = copy.deepcopy(input_config)
    config_params = config.setdefault('parameters', parameters)
    urlpath = config['args']['urlpath']
    template = jinja.from_string(urlpath)
    updated_path = template.render(config_params)
    config['args'].update({'urlpath': updated_path})
    return config


@task
def glob_all_files(config, storage_options={}):
    total_files = []
    data_path = config['args'].get('urlpath', None)
    if data_path is not None:
        if isinstance(data_path, list):
            for path in data_path:
                all_files = glob_url(path, **storage_options)
                total_files.append(all_files)
            total_files = list(it.chain.from_iterable(total_files))
        else:
            total_files = glob_url(data_path, **storage_options)
    return total_files


@task
def parse_raw_paths(all_raw_files, config):
    sonar_model = config.get('model')
    fname_pattern = config.get('raw_regex')
    return [
        dict(
            instrument=sonar_model,
            file_path=raw_file,
            **parse_file_path(raw_file, fname_pattern),
        )
        for raw_file in all_raw_files
    ]


@task
def print_raw_count(raw_dicts):
    print(f"There are {len(raw_dicts)} raw files.")  # noqa


@task
def export_raw_dicts(raw_dicts, export_path, export_storage_options={}):
    import json

    json_str = json.dumps(raw_dicts)
    with fsspec.open(export_path, mode="wt", **export_storage_options) as f:
        f.write(json_str)
