import os
from pathlib import Path
from typing import Any, Dict, List, Union
from echoflow.config.models.dataset import Dataset
from echoflow.config.models.output_model import Output
from echoflow.config.models.pipeline import Stage
from echoflow.stages_v2.aspects.echoflow_aspect import echoflow
from echoflow.stages_v2.utils.file_utils import download_temp_file, make_temp_folder
from echopype import open_raw, open_converted
from prefect import flow, task
from prefect_dask import DaskTaskRunner
from distributed import LocalCluster


@flow
@echoflow(processing_stage="open-raw", type="FLOW")
def echoflow_open_raw(config: Dataset, stage: Stage, data: Union[str, List[List[Dict[str, Any]]]]):

    if stage.options is not None and stage.options.get("out_path") is not None:
        if os.path.exists(os.path.join(stage.options.get("out_path"))) == False:
            print("creating")
            temp_raw_dir = make_temp_folder(stage.options.get("out_path"))
        else:
            temp_raw_dir = Path(stage.options.get("out_path"))
    elif config.output.urlpath is not None:
        if os.path.exists(os.path.join.join(config.output.urlpath)) == False:
            print("creating", config.output.urlpath)
            temp_raw_dir = make_temp_folder(config.output.urlpath+"/"+stage.name)
        else:
            temp_raw_dir = Path(config.output.urlpath+"/"+stage.name)
    else:
        temp_raw_dir = make_temp_folder("Echoflow_working_dir"+"/"+stage.name)

    ed_list = []
    futures = []
    outputs: List[Output] = []

    # Dealing with single file
    if type(data) == str or type(data) == Path:
        ed = process_raw(data, temp_raw_dir, config, stage)
        output = Output()
        output.data = ed
        outputs.append(output)
    else:
        for raw_dicts in data:
            for raw in raw_dicts:
                new_processed_raw = process_raw.with_options(
                    task_run_name=raw.get("file_path"), name=raw.get("file_path")
                )
                future = new_processed_raw.submit(raw, temp_raw_dir, config, stage)
                futures.append(future)

        ed_list = [f.result() for f in futures]

        transect_dict = {}
        print(ed_list) 
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
def process_raw(raw, temp_raw_dir: Path, dataset: Dataset, stage: Stage):
    temp_file = download_temp_file(raw, temp_raw_dir, stage)
    local_file = Path(temp_file.get("local_path"))
    out_zarr = os.path.join(temp_raw_dir, str(raw.get("transect_num")) , local_file.name.replace(".raw", ".zarr"))

    if stage.options.get("use_offline") == False and os.path.isfile(out_zarr) == False:
        ed = open_raw(raw_file=local_file, sonar_model=raw.get("instrument"))
        ed.to_zarr(
            save_path=str(out_zarr),
            overwrite=True,
            output_storage_options=dict(dataset.output.storage_options)
        )
        del ed
        if stage.options.get("save_output") == False:
            local_file.unlink()
    # ed = open_converted(
    #     converted_raw_path=str(out_zarr),
    #     storage_options=dict(dataset.output.storage_options),
    # )
    return {'out_zarr':out_zarr,'transect':raw.get("transect_num")}


# def open_raw(
#     raw_file: "PathHint",
#     sonar_model: "SonarModelsHint",
#     xml_path: Optional["PathHint"] = None,
#     convert_params: Optional[Dict[str, str]] = None,
#     storage_options: Optional[Dict[str, str]] = None,
#     use_swap: bool = False,
#     max_mb: int = 100,
# ) -> Optional[EchoData]:
