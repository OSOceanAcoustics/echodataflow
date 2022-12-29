from typing import Optional

from pydantic import BaseModel

from .calibrate import Calibrate


class Echopype(BaseModel):
    calibrate: Optional[Calibrate]
