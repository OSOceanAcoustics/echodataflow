from typing import List, Union
import echopype as ep
from echoflow.config.models.dataset import Dataset

from echoflow.config.models.output_model import Output
from echoflow.config.models.pipeline import Stage

from prefect import flow


@flow
def compute_MVBS(
        config: Dataset, stage: Stage, data: Union[str, List[Output]]
):
    outputs: List[Output] = []
    if(type(data) == list):
        for output_data in data:
            xr_d = ep.commongrid.compute_MVBS(
                ds_Sv = output_data.data,
                range_meter_bin = stage.external_params.get("range_meter_bin"), 
                ping_time_bin = stage.external_params.get("ping_time_bin")
                )
            new_output = Output()
            new_output.data = xr_d 
            outputs.append(new_output)
    return outputs
