from prefect import flow

from .tasks import (
    setup_config,
    glob_all_files,
    parse_raw_paths,
    print_raw_count,
    export_raw_dicts,
)


@flow
def find_raw_pipeline(
    config,
    parameters={},
    storage_options={},
    export=False,
    export_path='',
    export_storage_options={},
):
    new_config = setup_config(config, **parameters)
    total_files = glob_all_files(new_config, storage_options)
    file_dicts = parse_raw_paths(total_files, new_config)

    print_raw_count(file_dicts)

    if export:
        export_raw_dicts(file_dicts, export_path, export_storage_options)
    return file_dicts, new_config
