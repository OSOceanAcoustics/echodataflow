

from typing import Any, Dict, List, Union
from echoflow.config.models.dataset import Dataset
from echoflow.config.models.output_model import Output
from echoflow.config.models.pipeline import Stage
import echopype as ep

from prefect import flow,task

@task
def combine_echodata(
        config: Dataset, stage: Stage, data: Union[str, List[Output]], client=None
    ):
    outputs: List[Output] = []
    
    if(type(data) == list):
        for output_data in data:
            zarr_path = output_data.passing_params["zarr_path"]
            ceds = ep.combine_echodata(
                echodata_list = output_data.data
                # overwrite=config.output.overwrite,
                # storage_options=config.output.storage_options,
                # zarr_path = zarr_path
                )
            new_output = Output()
            new_output.data = ceds 
            outputs.append(new_output)

        return outputs
    
    

# def combine_echodata(
#     echodatas: List[EchoData] = None,
#     zarr_path: Optional[Union[str, Path]] = None,
#     overwrite: bool = False,
#     storage_options: Dict[str, Any] = {},
#     client: Optional[dask.distributed.Client] = None,
#     channel_selection: Optional[Union[List, Dict[str, list]]] = None,
#     consolidated: bool = True,
# ) -> EchoData:
