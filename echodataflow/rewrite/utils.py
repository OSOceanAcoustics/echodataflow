from pathlib import Path
import datetime
import numpy as np
import torch
from src.model.BinaryHakeModel import BinaryHakeModel


def get_MVBS_tensor(ds_in, freq_wanted=[120000, 38000, 18000]):
    # Find the right channel sequence
    ch_wanted = [int((np.abs(ds_in["frequency_nominal"]-freq)).argmin()) for freq in freq_wanted]

    mvbs_tensor = torch.tensor(
        (
            ds_in["Sv"]
            .transpose("channel", "depth", "ping_time")
            .isel(channel=ch_wanted).sel(depth=slice(None, 590)).values
        ),
        dtype=torch.float32
    )

    # Clip to Sv range
    mvbs_tensor_clip = torch.clip(
        mvbs_tensor.clone().detach().to(torch.float16),
        min=-70,
        max=-36,
    )

    # Replace NaN values with min Sv
    mvbs_tensor_clip[torch.isnan(mvbs_tensor_clip)] = -70

    # Normalize and conver to float
    mvbs_tensor_clip_normalized = (
        (mvbs_tensor_clip - (-70.0)) / (-36.0 - (-70.0)) * 255.0
    )
    
    return mvbs_tensor_clip_normalized.unsqueeze(0).float()


# Load binary hake models with weights
def get_hake_model(model_path: str) -> BinaryHakeModel:
    model = BinaryHakeModel("placeholder_experiment_name",
                            Path("placeholder_score_tensor_dir"),
                            "placeholder_tensor_log_dir", 0).eval()
    model.load_state_dict(torch.load(model_path, map_location=torch.device('cpu'))["state_dict"])
    return model


def round_up_minutes(dt: datetime.datetime, interval: int) -> datetime.datetime:
    """Round down datetime to nearest interval minutes.
    
    Parameters
    ----------
    dt : datetime.datetime
        Datetime to round
    interval : int
        Interval in minutes to round to
        
    Returns
    -------
    datetime.datetime
        Rounded datetime
    """
    # Round up minutes and handle day rollover
    minutes_since_midnight = dt.hour * 60 + dt.minute
    rounded_minutes = ((minutes_since_midnight + interval - 1) // interval) * interval
    
    days_to_add = rounded_minutes // (24 * 60)  # Number of days to add
    final_minutes = rounded_minutes % (24 * 60)  # Minutes in the final day
    
    return (dt
            .replace(hour=final_minutes // 60,
                    minute=final_minutes % 60,
                    second=0,
                    microsecond=0) 
            + datetime.timedelta(days=days_to_add))

def extract_mins(min_str: str) -> int:
    """
    Extract minutes from a string like '10mins'.
    """
    return int(min_str.replace("mins", ""))