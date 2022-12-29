from typing import Dict, Any, Optional
from .main import BaseModel


class ComputeSv(BaseModel):
    env_params: Dict[Any, Any] = {}
    cal_params: Dict[Any, Any] = {}
    waveform_mode: Dict[Any, Any] = {}


class Calibrate(BaseModel):
    compute_Sv: Optional[ComputeSv]
    compute_TS: Optional[Dict[Any, Any]]
