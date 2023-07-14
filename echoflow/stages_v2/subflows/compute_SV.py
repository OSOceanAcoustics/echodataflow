from typing import List, Union
import echopype as ep
from echoflow.config.models.dataset import Dataset

from echoflow.config.models.output_model import Output
from echoflow.config.models.pipeline import Stage

from prefect import flow, task

@task
def compute_SV(
        config: Dataset, stage: Stage, data: Union[str, List[Output]]
):
    outputs: List[Output] = []
    if(type(data) == list):
        for output_data in data:
            xr_d = ep.calibrate.compute_Sv(
                echodata = output_data.data
                )
            new_output = Output()
            new_output.data = xr_d 
            outputs.append(new_output)
    return outputs
