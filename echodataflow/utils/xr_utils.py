from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
import torch
import xarray as xr

from echodataflow.models.datastore import Dataset
from echodataflow.models.output_model import Group
from echodataflow.utils import log_util


def fetch_slice_from_store(edf_group: Group, config: Dataset, options: Dict[str, Any] = None, start_time: str = None, end_time: str = None, group: bool = False) -> xr.Dataset:
    default_options = {
                "engine":"zarr",
                "combine":"nested",
                "data_vars":"different",
                "coords":"different",
                "concat_dim": "ping_time",
                "storage_options": config.args.storage_options_dict}
    if options:
        default_options.update(options)
    
    store = None
    
    for ed in edf_group.data:
        if ed.data is not None:
            store = ed.data
        else:
            store = xr.open_mfdataset(paths=[ed.out_path for ed in edf_group.data], **default_options).compute()
            
    store_slice = store.sel(ping_time=slice(pd.to_datetime(start_time, unit="ns"), pd.to_datetime(end_time, unit="ns")))
    
    if store_slice["ping_time"].size == 0:
        del store
        del store_slice
        raise ValueError(f"No data available between {start_time} and {end_time}")
    
    if group:
        
        store_slice_grouped = store_slice.sortby('ping_time')

        for var in store_slice.data_vars:
            if 'ping_time' in store_slice[var].dims:
                store_slice_grouped[var] = store_slice[var].groupby('ping_time').mean()
                
        store_slice = store_slice_grouped
        del store_slice_grouped
    
    del store
    
    return store_slice

def assemble_da(data_array: xr.DataArray, dims: Dict[str, Any]):
    da = xr.DataArray(
        data_array, dims=dims.keys()
    )
    da = da.assign_coords(dims
    )
    return da

def process_xrd(ds: xr.Dataset, freq_wanted = [120000, 38000, 18000]) -> xr.Dataset:
    ds = ds.sel(depth=slice(None, 590))     
        
    ch_wanted = [int((np.abs(ds["frequency_nominal"]-freq)).argmin()) for freq in freq_wanted]
    ds = ds.isel(
                channel=ch_wanted
            )
    return ds

def combine_datasets(store_18: xr.Dataset, store_5: xr.Dataset) -> Tuple[torch.Tensor, xr.Dataset]:
    ds_32k_120k = None
    ds_18k = None
    combined_ds = None
    try:
        partial_channel_name = ["ES18"]
        ds_18k = extract_channels(store_18, partial_channel_name)        
        partial_channel_name = ["ES38", "ES120"]
        ds_32k_120k = extract_channels(store_5, partial_channel_name)
    except Exception as e:
        partial_channel_name = ["ES18"]
        ds_18k = extract_channels(store_5, partial_channel_name)
        partial_channel_name = ["ES38", "ES120"]
        ds_32k_120k = extract_channels(store_18, partial_channel_name)
        
    if not ds_18k or not ds_32k_120k:
        raise ValueError("Could not find the required channels in the datasets")
    
    ds_18k = process_xrd(ds_18k, freq_wanted=[18000])
    ds_32k_120k = process_xrd(ds_32k_120k, freq_wanted=[120000, 38000])
    
    combined_ds = xr.merge([ds_18k["Sv"], ds_32k_120k["Sv"], 
                            ds_18k['latitude'], ds_18k['longitude'],
                            ds_18k["frequency_nominal"], ds_32k_120k["frequency_nominal"]
                            ])
    combined_ds.attrs = ds_32k_120k.attrs

    
    return convert_to_tensor(combined_ds=combined_ds, config=None)

def convert_to_tensor(combined_ds: xr.Dataset, config: Dataset, freq_wanted: List[int] = [120000, 38000, 18000]) -> Tuple[xr.Dataset, torch.Tensor]:
    """
    Convert dataset to a tensor and return the tensor and the dataset.
    """
    
    ch_wanted = [int((np.abs(combined_ds["frequency_nominal"]-freq)).argmin()) for freq in freq_wanted]
    
    log_util.log(
        msg={"msg": f"Channel order {ch_wanted}", "mod_name": __file__, "func_name": "xr_utils.convert_to_tensor"},
        use_dask=False,
        eflogging=config.logging,
    )

    depth = combined_ds['depth']
    ping_time = combined_ds['ping_time']

    # Create a tensor with R=120 kHz, G=38 kHz, B=18 kHz mapping
    red_channel = extract_channels(combined_ds, ["ES120"])
    green_channel = extract_channels(combined_ds, ["ES38"])
    blue_channel = extract_channels(combined_ds, ["ES18"])

    ds = xr.concat([red_channel, green_channel, blue_channel], dim='channel')
    ds['channel'] = ['R', 'G', 'B']
    ds = ds.assign_coords({'depth': depth, 'ping_time': ping_time})

    ds = (
        ds
        .transpose("channel", "depth", "ping_time")
    )
    
    ds = ds.sel(depth=slice(None, 590))
    
    mvbs_tensor = torch.tensor(ds['Sv'].values, dtype=torch.float32)
    
    da_MVBS_tensor = torch.clip(
        mvbs_tensor.clone().detach().to(torch.float16),
        min=-70,
        max=-36,
    )
    log_util.log(
        msg={"msg": f"converted and clipped tensor", "mod_name": __file__, "func_name": "xr_utils.convert_to_tensor"},
        use_dask=False,
        eflogging=config.logging,
    )
    
    # Replace NaN values with min Sv
    da_MVBS_tensor[torch.isnan(da_MVBS_tensor)] = -70
    
    MVBS_tensor_normalized = (
        (da_MVBS_tensor - (-70.0)) / (-36.0 - (-70.0)) * 255.0
    )
    input_tensor = MVBS_tensor_normalized.unsqueeze(0).float()
    log_util.log(
        msg={"msg": f"Normalized tensor", "mod_name": __file__, "func_name": "xr_utils.convert_to_tensor"},
        use_dask=False,
        eflogging=config.logging,
    )
        
    return (ds, input_tensor)
    

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