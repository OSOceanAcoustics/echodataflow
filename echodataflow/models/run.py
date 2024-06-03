"""
Run Model for Echodataflow

This module defines a Pydantic model for representing a run in Echodataflow.

Classes:
    

Author: Soham Butala
Email: sbutala@uw.edu
Date: May 31, 2024
"""

from datetime import datetime
from typing import List

from pydantic import BaseModel

class EDFRun(BaseModel):
    last_run_time: str = datetime.min.isoformat()
    processed_files: List[str] = []