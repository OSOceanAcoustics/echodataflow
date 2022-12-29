from typing import Dict, Any, Optional, Literal
from pydantic import BaseModel


class ComputeCal(BaseModel):
    env_params: Optional[Dict[Any, Any]]
    cal_params: Optional[Dict[Any, Any]]
    waveform_mode: Optional[Literal["CW", "BB"]]
    encode_mode: Optional[Literal["complex", "power"]]


class Calibrate(BaseModel):
    compute_Sv: Optional[ComputeCal]
    compute_TS: Optional[ComputeCal]
