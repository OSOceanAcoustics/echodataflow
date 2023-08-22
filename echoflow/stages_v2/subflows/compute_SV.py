import os
from typing import Any, Dict, List, Union
import echopype as ep
from echoflow.config.models.datastore import Dataset

from echoflow.config.models.output_model import Output
from echoflow.config.models.pipeline import Stage
from echoflow.stages_v2.aspects.echoflow_aspect import echoflow

from prefect import flow, task

from echoflow.stages_v2.utils.file_utils import get_ed_list, get_working_dir


@flow
@echoflow(processing_stage="compute-sv", type="FLOW")
def echoflow_compute_SV(config: Dataset, stage: Stage, data: List[Output]):
    outputs: List[Output] = []
    futures = []
    working_dir = get_working_dir(config=config, stage=stage)
    # [Output(data=[{'out_path': '/Users/soham/Desktop/EchoWorkSpace/echoflow/notebooks/combined_files/echoflow_combine_echodata/7.zarr', 'transect': 7}], passing_params={}), Output(data=[{'out_path': '/Users/soham/Desktop/EchoWorkSpace/echoflow/notebooks/combined_files/echoflow_combine_echodata/10.zarr', 'transect': 10}], passing_params={})]

    if type(data) == list:
        if type(data[0].data) == list:
            for output_data in data:
                transect = str(output_data.data[0].get("transect")) + ".SV"
                print(transect)
                process_compute_SV_wo = process_compute_SV.with_options(
                    name=transect, task_run_name=transect
                )
                future = process_compute_SV_wo.submit(
                    config=config, stage=stage, out_data=output_data.data, working_dir=working_dir
                )
                futures.append(future)
        else:
            future = process_compute_SV.submit(
                config=config, stage=stage, out_data=data, working_dir=working_dir
            )
            futures.append(future)

        outputs = [f.result() for f in futures]
    return outputs


@task
def process_compute_SV(
    config: Dataset, stage: Stage, out_data: Union[List[Dict], List[Output]], working_dir: str
):
    ed_list = []
    xr_d_list = []
    if type(out_data) == list and type(out_data[0]) == dict:
        ed_list = get_ed_list.fn(config=config, stage=stage, transect_data=out_data)
        out_zarr = os.path.join(working_dir, str(out_data[0].get("transect")) + ".SV")
    else:
        for output_obj in out_data:
            ed_list.extend(get_ed_list.fn(config=config, stage=stage, transect_data=output_obj))
        out_zarr = os.path.join(working_dir, +"Default_Transect.SV")

    for ed in ed_list:
        xr_d = ep.calibrate.compute_Sv(echodata=ed)
        xr_d_list.append(xr_d)

    output = Output()
    output.data = xr_d_list

    print(output)

    # ceds.to_zarr(
    #     save_path=out_zarr,
    #     overwrite=True,
    #     output_storage_options=dict(config.output.storage_options),
    #     compute=False,
    # )
    # del ceds
    return output
