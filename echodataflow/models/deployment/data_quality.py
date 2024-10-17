from typing import List, Optional
from pydantic import BaseModel, Field, StrictBool

from echodataflow.models.deployment.storage_options import StorageOptions



class DataQualityCheck(BaseModel):
    name: str = Field(..., description="Name of the data quality check.")


class DataQuality(BaseModel):
    checks: Optional[List[DataQualityCheck]] = Field(None, description="List of data quality checks.")
    block_downstream: StrictBool = Field(False, description="Block downstream processes on failure.")
    out_path: Optional[str] = Field(None, description="Output path for data quality results.")
    storage_options: Optional[StorageOptions] = Field(None, description="Storage options for data quality results.")