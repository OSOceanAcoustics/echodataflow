import os
from pathlib import Path
from typing import Any, Dict, List, Union

from echoflow.config.models.datastore import Dataset
from echoflow.config.models.output_model import Output
from echoflow.config.models.pipeline import Stage
from echoflow.stages_v2.aspects.echoflow_aspect import echoflow
from echoflow.stages_v2.utils.file_utils import download_temp_file, isFile, get_working_dir
from echopype import open_raw, open_converted
from prefect import flow, task
from prefect_dask import DaskTaskRunner, get_dask_client
from distributed import LocalCluster

@flow
@echoflow(processing_stage="open-raw", type="FLOW")
def echoflow_open_raw(config: Dataset, stage: Stage, data: Union[str, List[List[Dict[str, Any]]]]):


    working_dir = get_working_dir(stage=stage, config=config)

    ed_list = []
    futures = []
    outputs: List[Output] = []

    # Dealing with single file
    if type(data) == str or type(data) == Path:
        ed = process_raw(data, working_dir, config, stage)
        output = Output()
        output.data = ed
        outputs.append(output)
    else:
        for raw_dicts in data:
            for raw in raw_dicts:
                new_processed_raw = process_raw.with_options(
                    task_run_name=raw.get("file_path"), name=raw.get("file_path")
                )
                future = new_processed_raw.submit(raw, working_dir, config, stage)
                futures.append(future)

        ed_list = [f.result() for f in futures]

        transect_dict = {}
        for ed in ed_list:
            transect = ed['transect']
            if transect in transect_dict:
                transect_dict[transect].append(ed)
            else:
                transect_dict[transect] = [ed]
            
        for transect in transect_dict.keys():
            output = Output()
            output.data = transect_dict[transect]
            outputs.append(output)

    return outputs


@task()
def process_raw(raw, working_dir: str, config: Dataset, stage: Stage):

    temp_file = download_temp_file(raw, working_dir, stage, config)
    local_file = temp_file.get("local_path")
    local_file_name = os.path.basename(temp_file.get("local_path"))
    out_zarr = os.path.join(working_dir, str(raw.get("transect_num")) , local_file_name.replace(".raw", ".zarr"))
    if stage.options.get("use_offline") == False or isFile(out_zarr) == False:
        print("OPENING RAW")
        ed = open_raw(raw_file=local_file, sonar_model=raw.get("instrument"), storage_options=config.output.storage_options_dict)
        ed.to_zarr(
                    save_path=str(out_zarr),
                    overwrite=True,
                    output_storage_options=config.output.storage_options_dict,
                    compute=False
        )
        del ed

        if stage.options.get("save_output") == False:
            local_file.unlink()

    return {'out_path':out_zarr,'transect':raw.get("transect_num")}


# def open_raw(
#     raw_file: "PathHint",
#     sonar_model: "SonarModelsHint",
#     xml_path: Optional["PathHint"] = None,
#     convert_params: Optional[Dict[str, str]] = None,
#     storage_options: Optional[Dict[str, str]] = None,
#     use_swap: bool = False,
#     max_mb: int = 100,
# ) -> Optional[EchoData]:
