import os
from pathlib import Path
from typing import Any, Dict, List, Union
from echoflow.config.models.dataset import Dataset
from echoflow.config.models.log_object import Log, Process
from echoflow.config.models.output_model import Output
from echoflow.config.models.pipeline import Stage
from echoflow.stages_v2.utils.file_utils import download_temp_file, get_output_file_path, make_temp_folder
from echoflow.stages_v2.utils.global_db_logger import add_new_process
from echoflow.stages_v2.utils.parallel_utils import get_client
import echopype as ep
from prefect import flow, task
from prefect_dask.task_runners import DaskTaskRunner
from distributed.lock import Lock

@flow
def open_raw(
    config: Dataset, stage: Stage, data: Union[str, List[List[Dict[str, Any]]]], client=None
):
    temp_raw_dir = make_temp_folder("offline_files")
    edList = []
    futures = []
    raw_dicts_futures = []
    i = 0
    outputs: List[Output] = []

    # Dealing with single file
    if (type(data) == str or type(data) == Path):
        ed = process_raw(data, temp_raw_dir, config, stage)
        edList.append(ed)
    else:
        if os.path.exists("/".join(config.output.urlpath)) == False:
            make_temp_folder(config.output.urlpath)
            os.chmod("/".join([config.output.urlpath]), 0o777)
        client = get_client(client)
        for raw_dicts in data:
            for raw in raw_dicts:
                new_processed_raw = process_raw.with_options(task_run_name=raw.get("file_path"))
                future = new_processed_raw.submit(
                    raw,
                    temp_raw_dir,
                    config,
                    stage
                )
                futures.append(future)
            raw_dicts_futures.append(futures)

    i = 0
    for futures in raw_dicts_futures:
        zarr_path = get_output_file_path(data[i], config)
        passing_params = {'zarr_path':zarr_path}
        output = Output()
        edList = [f.result() for f in futures]
        output.data = edList
        output.passing_params = passing_params
        outputs.append(output)
        i = i+1
    
    return outputs

@task
def process_raw(raw, temp_raw_dir, dataset:Dataset, stage: Stage):
    # print("Processing : ", raw.get("file_path"))

    fname = raw.get("file_path")
    process = Process(name=stage.name)

    try:
        temp_file = download_temp_file(raw, temp_raw_dir, stage)
        local_file = Path(temp_file.get("local_path"))
        ed = ep.open_raw(raw_file=local_file, sonar_model=raw.get("instrument"))

        out_zarr = "/".join(
            [dataset.output.urlpath, local_file.name.replace(".raw", ".zarr")]
        )

        with Lock("to_zarr.lock"):
            ed.to_zarr(
                save_path=str(out_zarr),
                overwrite=True,
                output_storage_options = dict(dataset.output.storage_options),
            )

        # Delete temp zarr
        del ed
        if stage.options.get("save_output") == False:
            # Delete temp raw
            local_file.unlink()
        dataset.args.zarr_store = out_zarr

        ed = ep.open_converted(
            converted_raw_path=str(out_zarr),
            storage_options=dict(dataset.output.storage_options),
        )
        # print("Completed processing : ", raw.get("file_path"))
        return ed
    except Exception as e:
        process.error = e
        raise e
    finally:
        process.status = True
        add_new_process(name=fname, process=process)


# def open_raw(
#     raw_file: "PathHint",
#     sonar_model: "SonarModelsHint",
#     xml_path: Optional["PathHint"] = None,
#     convert_params: Optional[Dict[str, str]] = None,
#     storage_options: Optional[Dict[str, str]] = None,
#     use_swap: bool = False,
#     max_mb: int = 100,
# ) -> Optional[EchoData]:
