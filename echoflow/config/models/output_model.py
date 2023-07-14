from typing import Any, Dict, Optional
from pydantic import BaseModel

class Output(BaseModel):
    data: Any
    passing_params: Dict[str, Any] = {}