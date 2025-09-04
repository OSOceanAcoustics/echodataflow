"""
Deploy the cloud data processing flows using Prefect.
"""
import re
from yaml import safe_load
import datetime
from pathlib import Path

import pandas as pd
import configparser
import s3fs

from prefect import deploy
from prefect.variables import Variable
from prefect.events import DeploymentEventTrigger

from flows_biology import flow_ingest_haul
from flows_integration import flow_ingest_NASC, flow_update_grid
from flows_viz_cloud import flow_update_cache_MVBS
# from flows_integration import flow_test_trigger, flow_to_be_trigger


if __name__ == "__main__":

    # Load variables from config
    with open(Path(__file__).parent / "config_cloud.yaml", "r") as file:
        config = safe_load(file)

    # Set init variables
    init_dict = config.pop("init")
    Variable.set("flow_start_time", init_dict["flow_start_time"], overwrite=True)
    Variable.set("counter_raw_copy", init_dict["counter_raw_copy"], overwrite=True)
    if init_dict["flow_start_time"] is None:
        curr_time_offset = datetime.timedelta(seconds=0)
    else:
        # Do all calculations in UTC
        curr_time_offset = (
            datetime.datetime.now(datetime.timezone.utc)
            - (
                datetime.datetime.fromisoformat(init_dict["flow_start_time"])
                .astimezone(datetime.timezone.utc)
            )
        )

    # Set interval dict
    interval_dict = {}
    for flow_name in config.keys():
        if flow_name != "init":
            interval_dict[flow_name] = config[flow_name].pop("interval", None)

    # Add time_offset_seconds to update_cache_MVBS config dict
    for flow_name in ["update_cache_MVBS"]:
        config[flow_name]["time_offset_seconds"] = curr_time_offset.total_seconds()

    # Deploy flows
    deploy(
        flow_ingest_haul.from_source(
            source=str(Path(__file__).parent),
            entrypoint="flows_biology.py:flow_ingest_haul"
        ).to_deployment(
            name="ingest_haul",
            parameters=config["ingest_haul"],
            # cron=f"*/{interval_dict["ingest_haul"]} * * * *",
        ),
        # flow_ingest_NASC.from_source(
        #     source=str(Path(__file__).parent),
        #     entrypoint="flows_integration.py:flow_ingest_NASC"
        # ).to_deployment(
        #     name="ingest_NASC",
        #     parameters=config["ingest_NASC"],
        #     # cron=f"*/{interval_dict["ingest_haul"]} * * * *",
        # ),
        # flow_update_grid.from_source(
        #     source=str(Path(__file__).parent),
        #     entrypoint="flows_integration.py:flow_update_grid"
        # ).to_deployment(
        #     name="update_grid",
        #     parameters=config["update_grid"],
        #     # cron=f"*/{interval_dict["ingest_haul"]} * * * *",
        #     triggers=[
        #         DeploymentEventTrigger(
        #             expect={"haul.ingested"},  # trigger on custom event
        #             match_related={"prefect.resource.name": "ingest_haul"},
        #         ),
        #         DeploymentEventTrigger(
        #             expect={"nasc.ingested"},  # trigger on custom event
        #             match_related={"prefect.resource.name": "ingest_NASC"},
        #         ),
        #     ]
        # ),
        flow_update_cache_MVBS.from_source(
            source=str(Path(__file__).parent),
            entrypoint="flows_viz_cloud.py:flow_update_cache_MVBS"
        ).to_deployment(
            name="update_cache_MVBS",
            parameters=config["update_cache_MVBS"],
            cron=f"3-59/{interval_dict['update_cache_MVBS']} * * * *",
        ),

        # flow_test_trigger.from_source(
        #     source=str(Path(__file__).parent),
        #     entrypoint="flows_integration.py:flow_test_trigger"
        # ).to_deployment(
        #     name="test_trigger",
        # ),
        # flow_to_be_trigger.from_source(
        #     source=str(Path(__file__).parent),
        #     entrypoint="flows_integration.py:flow_to_be_trigger"
        # ).to_deployment(
        #     name="to_be_trigger",
        #     triggers=[
        #         DeploymentEventTrigger(
        #             expect={"test.processed"},
        #             match_related={"prefect.resource.name": "test_trigger"},
        #         ),
        #     ]
        # ),
        work_pool_name="local",
    )
