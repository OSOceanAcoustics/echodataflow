"""
Deploy the ship data processing flows using Prefect.
"""

from yaml import safe_load
import datetime
from pathlib import Path

from prefect import deploy
from prefect.variables import Variable

from flows_acoustics import (
    # flow_copy_raw,
    flow_raw2Sv,
    flow_create_MVBS,
    flow_predict_hake,
)
from helpers import flow_file_upload



if __name__ == "__main__":

    # Load variables from config
    with open(Path(__file__).parent / "config_ship.yaml", "r") as file:
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

    # Add time_offset_seconds to create_MVBS and predict_hake config dict
    for flow_name in ["create_MVBS", "predict_hake"]:
        config[flow_name]["time_offset_seconds"] = curr_time_offset.total_seconds()

    deploy(
        # flow_copy_raw.from_source(
        #     source=str(Path(__file__).parent),
        #     entrypoint="flows_acoustics.py:flow_copy_raw"
        # ).to_deployment(
        #     name="copy-raw",
        #     # cron=f"*/{interval_dict["copy_raw"]} * * * *",
        # ),
        flow_raw2Sv.from_source(
            source=str(Path(__file__).parent),
            entrypoint="flows_acoustics.py:flow_raw2Sv",
        ).to_deployment(
            name="raw2Sv_test",
            parameters=config["raw2Sv"],
            # cron=f"*/{interval_dict["raw2Sv"]} * * * *",
        ),
        flow_create_MVBS.from_source(
            source=str(Path(__file__).parent),
            entrypoint="flows_acoustics.py:flow_create_MVBS",
        ).to_deployment(
            name="create-MVBS_test",
            parameters=config["create_MVBS"],
            # cron=f"3-59/{interval_dict["create_MVBS"]} * * * *",
        ),
        flow_predict_hake.from_source(
            source=str(Path(__file__).parent),
            entrypoint="flows_acoustics.py:flow_predict_hake",
        ).to_deployment(
            name="predict-hake_test",
            parameters=config["predict_hake"],
            # cron=f"*/{interval_dict["predict_hake"]} * * * *",
        ),
        work_pool_name="local",
    )
    # flow_file_upload.from_source(
    #     source=str(Path(__file__).parent),
    #     entrypoint="helpers.py:flow_file_upload",
    # ).to_deployment(
    #     name="file-upload-acoustics_test",
    #     parameters=config["file_upload_acoustics"],
    #     work_pool_name="local",
    #     cron=f"*/{interval_dict["file_upload_acoustics"]} * * * *",
    # ).apply()
    # flow_file_upload.from_source(
    #     source=str(Path(__file__).parent),
    #     entrypoint="helpers.py:flow_file_upload",
    # ).to_deployment(
    #     name="file-upload-trawl_test",
    #     parameters=config["file_upload_trawl"],
    #     cron=f"*/{interval_dict["file_upload_trawl"]} * * * *",
    #     work_pool_name="local",
    # ).apply()
