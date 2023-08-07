import os
from pathlib import Path
from typing import Any, Dict, List, Union
from echoflow.config.models.dataset import Dataset
from echoflow.config.models.output_model import Output
from echoflow.config.models.pipeline import Stage
from echopype import open_converted, combine_echodata, echodata

from prefect import task, flow

from echoflow.stages_v2.utils.file_utils import make_temp_folder


@flow
def echoflow_combine_echodata(config: Dataset, stage: Stage, data: Union[str, List[Output]], client=None):
    futures = []
    outputs: List[Output] = []

    if stage.options is not None and stage.options.get("out_path") is not None:
        if os.path.exists(os.path.join(stage.options.get("out_path"))) == False:
            print("creating")
            temp_raw_dir = make_temp_folder(stage.options.get("out_path"))
        else:
            temp_raw_dir = Path(stage.options.get("out_path"))
    elif config.output.urlpath is not None:
        if os.path.exists(os.path.join(config.output.urlpath)) == False:
            print("creating", config.output.urlpath)
            temp_raw_dir = make_temp_folder(config.output.urlpath+"/"+stage.name)
        else:
            temp_raw_dir = Path(config.output.urlpath+"/"+stage.name)
    else:
        temp_raw_dir = make_temp_folder("Echoflow_working_dir"+"/"+stage.name)

    if type(data) == list:
        if type(data[0].data) == list:
            for output_obj in data:
                output = Output()
                output = process_combine_echodata(config=config, stage=stage, out_data=output_obj.data, temp_raw_dir=temp_raw_dir)
                outputs.append(output)
        else:
            output = Output()
            output = process_combine_echodata(config=config, stage=stage, out_data=data)
            outputs.append(output)


            # if ed_list is None:
            #     ed_list = get_ed_list(config=config, stage=stage, output_obj=output_obj)
            # else:
            #     ed_list.extend(get_ed_list(config=config, stage=stage, output_obj=output_obj))
            # name = "Transect : "+ str(output_data.passing_params.get("transect"))
            # process_combine_echodata.with_options(name=name, task_run_name=name)
            # comb_future = process_combine_echodata.fn(
            #     config, stage, output_data, client
            #     )
            # futures.append(comb_future)

    # outputs = [f for f in futures]

    return outputs


@task
def process_combine_echodata(config: Dataset, stage: Stage, out_data: Union[List[Dict], List[Output]], temp_raw_dir : Path, transect = "Default", client=None):
    ed_list = []
    if type(out_data) == list and type(out_data[0]) == dict:
        ed_list = get_ed_list.fn(config=config, stage=stage, transect_data=out_data)
        out_zarr = os.path.join(temp_raw_dir, str(out_data[0].get("transect"))+".zarr")
        ceds = combine_echodata(echodata_list=ed_list)
        output = Output()
        output.data = ceds
    else:
        for output_obj in out_data:
            ed_list.extend(get_ed_list.fn(config=config, stage=stage, transect_data=output_obj))
        ceds = combine_echodata(echodata_list=ed_list)
        output = Output()
        output.data = ceds

    print("Storing at : ",out_zarr)
    ceds.to_zarr(save_path=out_zarr, overwrite=True, output_storage_options=dict(config.output.storage_options))

    return output


@task
def get_ed_list(config: Dataset, stage: Stage, transect_data: Union[Output, List[Dict]]):
    ed_list = []
    if type(transect_data) == list:
        for zarr_path_data in transect_data:
            ed = open_converted(
                converted_raw_path=str(zarr_path_data.get("out_zarr")),
                storage_options=dict(config.output.storage_options)
            )
            ed_list.append(ed)
    else:
        zarr_path_data = transect_data.data
        ed = open_converted(
                converted_raw_path=str(zarr_path_data.get("out_zarr")),
                storage_options=dict(config.output.storage_options)
        )
        ed_list.append(ed)
    return ed_list


# def combine_echodata(
#     echodatas: List[EchoData] = None,
#     zarr_path: Optional[Union[str, Path]] = None,
#     overwrite: bool = False,
#     storage_options: Dict[str, Any] = {},
#     client: Optional[dask.distributed.Client] = None,
#     channel_selection: Optional[Union[List, Dict[str, list]]] = None,
#     consolidated: bool = True,
# ) -> EchoData:
