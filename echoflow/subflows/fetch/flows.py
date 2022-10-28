from prefect import flow

from .tasks import export_raw_dicts, glob_all_files, parse_raw_paths, setup_config


@flow
def find_raw_pipeline(
    config,
    parameters={},
    storage_options={},
    export=False,
    export_path="",
    export_storage_options={},
):
    new_config = setup_config(config, **parameters)
    total_files = glob_all_files(new_config, storage_options)
    file_dicts = parse_raw_paths(total_files, new_config)

    if export:
        export_raw_dicts(file_dicts, export_path, export_storage_options)
    return file_dicts, new_config
