from typing import Optional

from pydantic import BaseModel

from .calibrate import Calibrate


class EchopypeConfig(BaseModel):
    calibrate: Optional[Calibrate]
