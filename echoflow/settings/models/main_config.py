from typing import Optional

from .raw_config import RawConfig
from .echopype_config import EchopypeConfig


class MainConfig(RawConfig):
    echopype: Optional[EchopypeConfig]
