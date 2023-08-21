from typing import Any, Dict, List, Optional
from prefect.blocks.core import Block
from pydantic import SecretStr

class EchoflowPrefectConfig(Block):
    prefect_api_key: Optional[SecretStr] = None
    prefect_account_id: Optional[SecretStr] = None
    prefect_workspace_id: Optional[SecretStr] = None
    profile_name: str = None

    def get_api_url(self):
        if self.prefect_api_key is not None:
            return f"https://api.prefect.cloud/api/accounts/{ self.prefect_account_id.get_secret_value() }/workspaces/{ self.prefect_workspace_id.get_secret_value() }" 
        elif self.profile_name == "echoflow_prefect_local":
            return "127.0.0.1:4200"

class BaseConfig(Block):
    name: str
    type: str
    active: Optional[bool] = False
    options: Optional[Dict[str, Any]] = {}

class EchoflowConfig(Block):
    active: Optional[str] = None
    prefect_configs : Optional[List[str]]
    blocks: Optional[List[BaseConfig]] = []