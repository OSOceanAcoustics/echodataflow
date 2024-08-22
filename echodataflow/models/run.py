"""
Run Model for Echodataflow

This module defines a Pydantic model for representing a run in Echodataflow.

Classes:
    

Author: Soham Butala
Email: sbutala@uw.edu
Date: May 31, 2024
"""

from collections import defaultdict
from datetime import datetime
from typing import Dict, Optional
from prefect.blocks.core import Block

class FileDetails(Block):
    status: Optional[bool] = False
    process_timestamp: Optional[str] = datetime.now().isoformat()
    retry_count: Optional[int] = 0

class EDFRun(Block):
    last_run_time: Optional[str] = datetime.min.isoformat()
    processed_files: Optional[Dict[str, FileDetails]] = defaultdict(FileDetails)