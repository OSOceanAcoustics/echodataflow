from typing import List, Dict, Any, Optional
from prefect import flow

from echodataflow.models.deployment.stage import Stage

@flow(name="Echodataflow-Service")
def edf_service(stages: List[Stage], edf_logger: Optional[Dict[str, Any]]):
    """
    Service for EDF
    """
    pass