
"""
Echodataflow Mask_prediction Task

This module defines a Prefect Flow and associated tasks for the Echodataflow Mask_prediction stage.

Classes:
    None

Functions:
    echodataflow_mask_prediction(config: Dataset, stage: Stage, data: Union[str, List[Output]])
    process_mask_prediction(config: Dataset, stage: Stage, out_data: Union[List[Dict], List[Output]], working_dir: str)

Author: Soham Butala
Email: sbutala@uw.edu
Date: August 22, 2023
"""
from collections import defaultdict
from pathlib import Path
from typing import Dict, Optional, Union

import echopype as ep
from prefect import flow, task
import torch
import xarray as xr
import numpy as np

from echodataflow.aspects.echodataflow_aspect import echodataflow
from echodataflow.models.datastore import Dataset
from echodataflow.models.output_model import EchodataflowObject, ErrorObject, Group, Output
from echodataflow.models.pipeline import Stage
from echodataflow.utils import log_util
from echodataflow.utils.file_utils import get_ed_list, get_out_zarr, get_working_dir, get_zarr_list, isFile
from src.model.BinaryHakeModel import BinaryHakeModel


@flow
@echodataflow(processing_stage="mask-prediction", type="FLOW")
def echodataflow_mask_prediction(
        groups: Dict[str, Group], config: Dataset, stage: Stage, prev_stage: Optional[Stage]
):
    """
    mask prediction from echodata.

    Args:
        config (Dataset): Configuration for the dataset being processed.
        stage (Stage): Configuration for the current processing stage.
        prev_stage (Stage): Configuration for the previous processing stage.

    Returns:
        List[Output]: List of The input dataset with the mask prediction data added.

    Example:
        # Define configuration and data
        dataset_config = ...
        pipeline_stage = ...
        echodata_outputs = ...

        # Execute the Echodataflow mask_prediction stage
        mask_prediction_output = echodataflow_mask_prediction(
            config=dataset_config,
            stage=pipeline_stage,
            data=echodata_outputs
        )
        print("Output :", mask_prediction_output)
    """
    working_dir = get_working_dir(stage=stage, config=config)

    futures = defaultdict(list)

    for name, gr in groups.items():
        for ed in gr.data:
            gname = ed.out_path.split(".")[0] + ".MaskPrediction"
            new_process = process_mask_prediction.with_options(
                task_run_name=gname, name=gname, retries=3
                    )
            future = new_process.submit(
                ed=ed, working_dir=working_dir, config=config, stage=stage
                )
            futures[name].append(future)

    for name, flist in futures.items():
        try:
            groups[name].data = [f.result() for f in flist]
        except Exception as e:
            groups[name].data[0].error = ErrorObject(errorFlag=True, error_desc=str(e))

    return groups


@task
@echodataflow()
def process_mask_prediction(
    ed: EchodataflowObject, config: Dataset, stage: Stage, working_dir: str
):
    """
    Process and mask prediction from Echodata object into the dataset.

    Args:
        config (Dataset): Configuration for the dataset being processed.
        stage (Stage): Configuration for the current processing stage.
        out_data (Union[Dict, Output]): Processed outputs (xr.Dataset) to mask prediction.
        working_dir (str): Working directory for processing.

    Returns:
        The input dataset with the mask prediction data added

    Example:
        # Define configuration, processed data, and working directory
        dataset_config = ...
        pipeline_stage = ...
        processed_outputs = ...
        working_directory = ...

        # Process and mask prediction
        mask_prediction_output = process_mask_prediction(
            config=dataset_config,
            stage=pipeline_stage,
            out_data=processed_outputs,
            working_dir=working_directory
        )
        print(" Output :", mask_prediction_output)
    """

    file_name = ed.filename + "_mask.zarr"

    try:
        log_util.log(
            msg={"msg": " ---- Entering ----", "mod_name": __file__, "func_name": file_name},
            use_dask=stage.options["use_dask"],
            eflogging=config.logging,
        )

        out_zarr = get_out_zarr(
            group=stage.options.get("group", True),
            working_dir=working_dir,
            transect=ed.group_name,
            file_name=file_name,
            storage_options=config.output.storage_options_dict,
        )

        log_util.log(
            msg={
                "msg": f"Processing file, output will be at {out_zarr}",
                "mod_name": __file__,
                "func_name": file_name,
            },
            use_dask=stage.options["use_dask"],
            eflogging=config.logging,
        )

        if (
            stage.options.get("use_offline") == False
            or isFile(out_zarr, config.output.storage_options_dict) == False
        ):
            log_util.log(
                msg={
                    "msg": f"File not found in the destination folder / use_offline flag is False",
                    "mod_name": __file__,
                    "func_name": file_name,
                },
                use_dask=stage.options["use_dask"],
                eflogging=config.logging,
            )

            ed_list = get_zarr_list.fn(transect_data=ed, storage_options=config.output.storage_options_dict)

            log_util.log(
                msg={"msg": 'Computing mask_prediction', "mod_name": __file__, "func_name": file_name},
                use_dask=stage.options["use_dask"],
                eflogging=config.logging,
            )

            bottom_offset = stage.external_params.get('bottom_offset', 1.0)
            temperature = stage.external_params.get('temperature', 0.5)
            freq_wanted = stage.external_params.get('freq_wanted', [120000, 38000, 18000])
            
            ch_wanted = [int((np.abs(ed_list[0]["frequency_nominal"]-freq)).argmin()) for freq in freq_wanted]

            log_util.log(
                msg={"msg": f"Channel order {ch_wanted}", "mod_name": __file__, "func_name": file_name},
                use_dask=stage.options["use_dask"],
                eflogging=config.logging,
            )
            
            mvbs_slice = ed_list[0].isel(
                channel=ch_wanted
            )
            
            # Load binary hake models with weights
            model = BinaryHakeModel("placeholder_experiment_name",
                                    Path("placeholder_score_tensor_dir"),
                                    "placeholder_tensor_log_dir", 0).eval()
            model.load_state_dict(torch.load(f"/home/exouser/hake_data/model/backup_model_weights/binary_hake_model_{bottom_offset}m_bottom_offset_1.0m_depth_2017_2019_ver_1.ckpt")["state_dict"])            
            
            mvbs_tensor = torch.tensor(mvbs_slice['Sv'].values, dtype=torch.float32).unsqueeze(0)

            da_MVBS_tensor = torch.clip(
                torch.tensor(mvbs_tensor, dtype=torch.float16),
                min=-70,
                max=-36,
            )
            # Replace NaN values with min Sv
            da_MVBS_tensor[torch.isnan(da_MVBS_tensor)] = -36
            
            MVBS_tensor_normalized = (
                (da_MVBS_tensor - (-70.0)) / (-36.0 - (-70.0)) * 255.0
            )
            input_tensor = MVBS_tensor_normalized.float()
            
            score_tensor = model(input_tensor).detach().squeeze(0)
            
            log_util.log(
                msg={"msg": f"Converting to Zarr", "mod_name": __file__, "func_name": file_name},
                use_dask=stage.options["use_dask"],
                eflogging=config.logging,
            )

            dims = ['ping_time', 'depth']
            
            softmax_score_tensor = torch.nn.functional.softmax(
                score_tensor / temperature, dim=0
            )
            
            da_score_hake = assemble_da(score_tensor.numpy()[1,:,:], ds_ref=ed_list[0], dims=dims)
            da_softmax_hake = assemble_da(softmax_score_tensor.numpy()[1,:,:], ds_ref=ed_list[0], dims=dims)
            
            score_zarr = get_out_zarr(
                group=True,
                working_dir=working_dir,
                transect="Hake_Score",
                file_name=ed.filename + "_score_hake.zarr",
                storage_options=config.output.storage_options_dict,
            )
            
            softmax_zarr = get_out_zarr(
                group=True,
                working_dir=working_dir,
                transect="Softmax_Score",
                file_name=ed.filename + "_softmax_score.zarr",
                storage_options=config.output.storage_options_dict,
            )

            # save scores 
            da_score_hake.to_zarr(
                store=score_zarr,
                mode="w",
                consolidated=True,
                storage_options=config.output.storage_options_dict,
                )
            
            da_softmax_hake.to_zarr(
                store=softmax_zarr,
                mode="w",
                consolidated=True,
                storage_options=config.output.storage_options_dict,
                )
            
            # Get mask from softmax score
            da_mask_hake = assemble_da(score_tensor.max(dim=0)[1].int().numpy(), ds_ref=ed_list[0], dims=dims)
            
            da_mask_hake.to_zarr(
                store=out_zarr,
                mode="w",
                consolidated=True,
                storage_options=config.output.storage_options_dict,
            )
            
        else:
            log_util.log(
                msg={
                    "msg": f"Skipped processing {file_name}. File found in the destination folder. To replace or reprocess set `use_offline` flag to False",
                    "mod_name": __file__,
                    "func_name": file_name,
                },
                use_dask=stage.options["use_dask"],
                eflogging=config.logging,
            )

        log_util.log(
            msg={"msg": f" ---- Exiting ----", "mod_name": __file__, "func_name": file_name},
            use_dask=stage.options["use_dask"],
            eflogging=config.logging,
        )
        ed.stages["mask"] = out_zarr
        ed.error = ErrorObject(errorFlag=False)
        ed.stages[stage.name] = out_zarr
    except Exception as e:
        log_util.log(
            msg={"msg": "", "mod_name": __file__, "func_name": file_name},
            use_dask=stage.options["use_dask"],
            eflogging=config.logging,
            error=e
        )
        ed.error = ErrorObject(errorFlag=True, error_desc=e)
    finally:
        return ed


def assemble_da(data_array, ds_ref, dims):
    da = xr.DataArray(
        data_array, dims=dims
    )
    da = da.assign_coords(
        {dim: ds_ref[dim] for dim in dims}
    )
    return da