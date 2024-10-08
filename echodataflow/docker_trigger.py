from pathlib import Path
from echodataflow.stages.echodataflow_trigger import echodataflow_trigger
from prefect import flow
from prefect.task_runners import ThreadPoolTaskRunner
from typing import Any, Dict, Optional, Union

@flow(name="docker-trigger-latest", task_runner=ThreadPoolTaskRunner(max_workers=1))
def docker_trigger(
    dataset_config: Union[dict, str, Path],
    pipeline_config: Union[dict, str, Path],
    logging_config: Union[dict, str, Path] = None,
    storage_options: Optional[dict] = None,
    options: Optional[dict] = {},
    json_data_path: Union[str, Path] = None
):
    return echodataflow_trigger(
        dataset_config=dataset_config,
        pipeline_config=pipeline_config,
        logging_config=logging_config,
        storage_options=storage_options,
        options=options,
        json_data_path=json_data_path
    )

if __name__ == "__main__":
    docker_trigger.serve(name="docker-trigger-latest")