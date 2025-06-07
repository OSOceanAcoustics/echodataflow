"""
Deploy the cloud data processing flows using Prefect.
"""
import re
from yaml import safe_load
import datetime
from pathlib import Path

import pandas as pd

from prefect import deploy
from prefect.variables import Variable

from flows_biology import flow_ingest_haul


if __name__ == "__main__":

    # Load variables from config
    with open(Path(__file__).parent / "config.yaml", "r") as file:
        config = safe_load(file)

    # Set init variables
    init_dict = config.pop("init")
    Variable.set("flow_start_time", init_dict["flow_start_time"], overwrite=True)
    Variable.set("counter_raw_copy", init_dict["counter_raw_copy"], overwrite=True)
    if init_dict["flow_start_time"] is None:
        curr_time_offset = datetime.timedelta(seconds=0)
    else:
        curr_time_offset = (
            datetime.datetime.now()
            - datetime.datetime.strptime(
                init_dict["flow_start_time"], "%Y%m%dT%H%M%S"
            )
        )

    # Set interval dict
    interval_dict = {}
    for flow_name in config.keys():
        if flow_name != "init":
            interval_dict[flow_name] = config[flow_name].pop("interval", None)

    # # Inititalize empty bio dataframes if not already exist
    # path_main = Path(config["ingest_haul"]["path_main"])
    # csv_filename_keys = [k for k in config["ingest_haul"].keys() if re.match(r"path_\w+_all", k)]
    # for csv_k in csv_filename_keys:
    #     csvf = (path_main / config["ingest_haul"][csv_k]).with_suffix(".csv")
    #     if not csvf.exists():
    #         df = pd.DataFrame()
    #         df.to_csv(csvf, index=False)

    deploy(
        flow_ingest_haul.from_source(
            source=str(Path(__file__).parent),
            entrypoint="flows_biology.py:flow_ingest_haul"
        ).to_deployment(
            name="ingest_haul",
            parameters=config["ingest_haul"],
            cron=f"*/{interval_dict["ingest_haul"]} * * * *",
        ),
        work_pool_name="local",
    )
