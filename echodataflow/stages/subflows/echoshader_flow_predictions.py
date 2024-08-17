
"""
Echodataflow Resample Task

This module defines a Prefect Flow and associated tasks for the Echodataflow Resample stage.

Classes:
    None

Functions:
    echodataflow_resample(config: Dataset, stage: Stage, data: Union[str, List[Output]])
    process_resample(config: Dataset, stage: Stage, out_data: Union[List[Dict], List[Output]], working_dir: str)

Author: Soham Butala
Email: sbutala@uw.edu
Date: August 22, 2023
"""
from collections import defaultdict
import io
from typing import Dict, Optional, List

import matplotlib
import numpy as np
from prefect import flow, task
import xarray as xr
from echodataflow.aspects.echodataflow_aspect import echodataflow
from echodataflow.models.datastore import Dataset
from echodataflow.models.output_model import EchodataflowObject, ErrorObject, Group
from echodataflow.models.pipeline import Stage
from echodataflow.utils import log_util
from echodataflow.utils.file_utils import fetch_slice_from_store, get_out_zarr, get_working_dir, get_zarr_list, isFile
import panel as pn
from holoviews import opts
import echoshader
import diskcache as dc
import echopype.colormap

import scipy

@flow
@echodataflow(processing_stage="echoshader", type="FLOW")
def echoshader_flow_predictions(
        groups: Dict[str, Group], config: Dataset, stage: Stage, prev_stage: Optional[Stage]
):
    working_dir = get_working_dir(stage=stage, config=config)

    futures = defaultdict(list)

    print(len(groups.items()))
    

    for name, gr in groups.items():
        print(name)
        print(gr)
        if gr.metadata and gr.metadata.is_store_folder and len(gr.data) > 0:
            edf = gr.data[0]
            if edf.data is None:
                edf.data = fetch_slice_from_store(edf_group=gr, config=config, start_time=edf.start_time, end_time=edf.end_time)
                gr.data = [edf]

        for ed in gr.data:
            gname = ed.out_path.split(".")[0] + ".Visualize"
            processed_data = eshader_score_preprocess.with_options(
                task_run_name=gname, name=gname, retries=3
            ).submit(
                ed=ed, working_dir=working_dir, config=config, stage=stage
            )
            
            # future = eshader_visualize.with_options(
            #     task_run_name=gname, name=gname, retries=3
            # ).submit(
            #     ed=processed_data, config=config, stage=stage
            # )
            
            future = processed_data
            
            futures[name].append(future)
         
    for name, flist in futures.items():
        try:
            results = []
            for f in flist:
                res = f.result()
                results.append(res)
                del f
            groups[name].data = results
        except Exception as e:
            groups[name].data[0].error = ErrorObject(errorFlag=True, error_desc=str(e))
        del res
        del results
           
    
    return groups

@task
def eshader_score_preprocess(ed: EchodataflowObject, working_dir, config: Dataset, stage: Stage):
    
    # change to score
    file_name = ed.filename + "_MVBS.zarr"
    try:
        log_util.log(
            msg={"msg": " ---- Entering ----", "mod_name": __file__, "func_name": file_name},
            use_dask=stage.options["use_dask"],
            eflogging=config.logging,
        )



        score_combined = get_zarr_list.fn(
            transect_data=ed, storage_options=config.output.storage_options_dict
        )[0]
        
        score_combined.to_zarr(
            working_dir + "/" + "eshader.zarr", 
            mode="w", 
            consolidated=True,
            storage_options=config.output.storage_options_dict,
        )

        print(score_combined.shape)

        log_util.log(
            msg={"msg": f"Processing data for visualization", "mod_name": __file__, "func_name": file_name},
            use_dask=stage.options["use_dask"],
            eflogging=config.logging,
        )

        external_kwargs = stage.external_params    

        softmax_combined = xr.apply_ufunc(scipy.special.softmax, 
                                 score_combined, 
                                 kwargs = {'axis': 0}, 
                                 dask="allowed"
                                 )
        softmax_combined_resampled = softmax_combined.resample(ping_time="30s").mean()

               
        # change to softmax

        # change path to not be hard-coded
        cache = dc.Cache(stage.options.get('cache_location','/Users/valentina/projects/uw-echospace/echoshader_flow/eshader_cache_shimada'))
        cache.clear()
        cache.set('zarr_path', working_dir + "/" + "eshader.zarr")
        # cache.set('channel_multi_freq', [ch for ch in ds_MVBS_combined_resampled.channel.values if "ES38" in str(ch)])
        # cache.set('channel_tricolor', [ch for ch in ds_MVBS_combined_resampled.channel.values])
        # cache.set('tile_select', ds_MVBS_combined_resampled.eshader.tile_select)



        log_util.log(
            msg={"msg": f" ---- Exiting ----", "mod_name": __file__, "func_name": file_name},
            use_dask=stage.options["use_dask"],
            eflogging=config.logging,
        )
        
        if softmax_combined_resampled:
            del softmax_combined_resampled
        if ed.data_ref is not None:
            del ed.data_ref
            
            
    except Exception as e:

        print("An error occurred")
        log_util.log(
            msg={"msg": "", "mod_name": __file__, "func_name": file_name},
            use_dask=stage.options["use_dask"],
            eflogging=config.logging,
            error=e
        )
        ed.error = ErrorObject(errorFlag=True, error_desc=str(e))
        ed.data = None
        ed.data_ref = None
    finally:
        return ed
    
def eshader_multi_freq(ed: EchodataflowObject, config: Dataset, cmap):
    
    ds_MVBS = get_zarr_list.fn(
            transect_data=ed, storage_options=config.output.storage_options_dict, delete_from_edf=False
    )[0]
    
    clipping = {
        'min': tuple(cmap.get_under()),
        'max': tuple(cmap.get_over()),
        'NaN': tuple(cmap.get_bad()),
    }

    egram_all = ds_MVBS.sel(echo_range=slice(None, 400)).eshader.echogram(
        channel = [
            # 'WBT 400141-15 ES18_ES',
            'WBT 400143-15 ES38B_ES',
            # 'WBT 400140-15 ES120-7C_ES',
        ],
        vmin = -70, 
        vmax = -36,
        cmap = "ep.ek500", 
        opts = opts.Image(clipping_colors=clipping, width=800),
    )

    return egram_all

def eshader_tricolor(ed: EchodataflowObject, config: Dataset, cmap):
    
    ds_MVBS = get_zarr_list.fn(
            transect_data=ed, storage_options=config.output.storage_options_dict, delete_from_edf=False
    )[0]

    tricolor = ds_MVBS.sel(echo_range=slice(None, 400)).eshader.echogram(
        channel=[
            'WBT 400140-15 ES120-7C_ES', 'WBT 400143-15 ES38B_ES', 'WBT 400141-15 ES18_ES',
        ],
        vmin = -70,  vmax = -36, rgb_composite = True,
        opts = opts.RGB(width=800),
    )
    return tricolor

def eshader_track(ed: EchodataflowObject, config: Dataset, cmap):
    
    ds_MVBS = get_zarr_list.fn(
            transect_data=ed, storage_options=config.output.storage_options_dict, delete_from_edf=False
    )[0]
    
    track = ds_MVBS.eshader.track(
        tile = 'EsriOceanBase',
        opts = opts.Path(width=600, height=350),
    )

    tile_select: pn.widgets.Select = ds_MVBS.eshader.tile_select

    return (tile_select, track)
    
@task
def eshader_visualize(ed: EchodataflowObject, config: Dataset, stage: Stage):
    
    external_kwargs = stage.external_params
    viz_params = external_kwargs.get("viz_params", {}) if external_kwargs else {}
    
    cmap = matplotlib.colormaps[viz_params.get("color_map", "ep.ek500")]
    cache = dc.Cache(external_kwargs.get("cache_name", 'eshader_cache'))
        
    try:
        egram = eshader_multi_freq(ed, config, cmap)
        cache.set('egram', egram)
    except Exception as e:
        log_util.log(
            msg={"msg": "", "mod_name": __file__, "func_name": ed.filename},
            use_dask=stage.options["use_dask"],
            eflogging=config.logging,
            error=e
        )
        ed.error = ErrorObject(errorFlag=True, error_desc=str(e))
    
    try:
        tricolor = eshader_tricolor(ed, config, cmap)
        cache.set('tricolor', tricolor)
    except Exception as e:
        log_util.log(
            msg={"msg": "", "mod_name": __file__, "func_name": ed.filename},
            use_dask=stage.options["use_dask"],
            eflogging=config.logging,
            error=e
        )
        ed.error = ErrorObject(errorFlag=True, error_desc=str(e))
    
    try:
        tile_select, track = eshader_track(ed, config, cmap)
        cache.set('track', track)
        cache.set('tile_select', tile_select)
    except Exception as e:
        log_util.log(
            msg={"msg": "", "mod_name": __file__, "func_name": ed.filename},
            use_dask=stage.options["use_dask"],
            eflogging=config.logging,
            error=e
        )
        ed.error = ErrorObject(errorFlag=True, error_desc=str(e))
    
    if ed.data:
        del ed.data
    if ed.data_ref:
        del ed.data_ref
    return ed

def extract_channels(dataset: xr.Dataset, partial_names: List[str]) -> xr.Dataset:
    """
    Extracts multiple channels data from the given xarray dataset using partial channel names.

    Args:
        dataset (xr.Dataset): The input xarray dataset containing multiple channels.
        partial_names (List[str]): The list of partial names of the channels to extract.

    Returns:
        xr.Dataset: The dataset containing only the specified channels data.
    """
    matching_channels = []
    for partial_name in partial_names:
        matching_channels.extend([channel for channel in dataset.channel.values if partial_name in str(channel)])
    
    if len(matching_channels) == 0:
        raise ValueError(f"No channels found matching any of '{partial_names}'")
    
    return dataset.sel(channel=matching_channels)