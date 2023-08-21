import os
from pathlib import Path
from typing import Any, Dict, List, Union
from echoflow.config.models.datastore import Dataset
from echoflow.config.models.output_model import Output
from echoflow.config.models.pipeline import Stage
from echopype import open_converted, combine_echodata, echodata

from prefect import task, flow
from prefect_dask import get_dask_client
from echoflow.stages_v2.utils.file_utils import get_working_dir, get_ed_list, isFile
from dask.distributed import Client, LocalCluster

@flow
def echoflow_combine_echodata(
    config: Dataset, stage: Stage, data: List[Output]
):
    futures = []
    outputs: List[Output] = []

    working_dir = get_working_dir(config=config, stage=stage)

    if type(data) == list:
        if type(data[0].data) == list:
            for output_obj in data:
                transect = str(output_obj.data[0].get("transect")) + ".zarr"
                process_combine_echodata_wo = process_combine_echodata.with_options(
                    name=transect, task_run_name=transect
                )
                future = process_combine_echodata_wo.submit(
                    config=config, stage=stage, out_data=output_obj.data, working_dir=working_dir
                )
                futures.append(future)
        else:
            future = process_combine_echodata.submit(config=config, stage=stage, out_data=data, working_dir=working_dir)
            futures.append(future)

        outputs = [f.result() for f in futures]
    return outputs


@task
def process_combine_echodata(
    config: Dataset,
    stage: Stage,
    out_data: Union[List[Dict], List[Output]],
    working_dir: str
):
    ed_list = []
    if type(out_data) == list and type(out_data[0]) == dict:
        out_zarr = os.path.join(working_dir, str(out_data[0].get("transect")) + ".zarr")
        if stage.options.get("use_offline") == False or isFile(out_zarr) == False:
            ed_list = get_ed_list.fn(config=config, stage=stage, transect_data=out_data)
            ceds = combine_echodata(echodata_list=ed_list)
            ceds.to_zarr(
            save_path=out_zarr,
            overwrite=True,
            output_storage_options=dict(config.output.storage_options_dict),
            compute=False
            )
            del ceds
        output = Output()
        output.data = [{'out_path':out_zarr,'transect':out_data[0].get("transect")}]
    else:
        out_zarr = os.path.join(working_dir,  + "Default_Transect.zarr")
        if stage.options.get("use_offline") == False or isFile(out_zarr) == False:
            for output_obj in out_data:
                ed_list.extend(get_ed_list.fn(config=config, stage=stage, transect_data=output_obj))
            ceds = combine_echodata(echodata_list=ed_list)
            ceds.to_zarr(
            save_path=out_zarr,
            overwrite=True,
            output_storage_options=dict(config.output.storage_options_dict),
            compute=False
            )
            del ceds
        output = Output()
        output.data = [{'out_path':out_zarr,'transect':"Default_Transect"}]
    return output



# def combine_echodata(
#     echodatas: List[EchoData] = None,
#     zarr_path: Optional[Union[str, Path]] = None,
#     overwrite: bool = False,
#     storage_options: Dict[str, Any] = {},
#     client: Optional[dask.distributed.Client] = None,
#     channel_selection: Optional[Union[List, Dict[str, list]]] = None,
#     consolidated: bool = True,
# ) -> EchoData:
