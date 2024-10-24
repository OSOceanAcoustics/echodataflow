from enum import Enum
import functools
from typing import Optional
from pydantic import BaseModel, Field, StrictBool, model_validator


class StorageType(str, Enum):
    """
    Enumeration for storage types.

    Attributes:
        AWS: Amazon Web Services storage type.
        AZCosmos: Azure storage type.
        GCP: Google Cloud Platform storage type.
        ECHODATAFLOW: Echodataflow-specific storage type.
        EDFRUN: Echodataflow runtime storage type.
    """
    AWS = "AWS"
    AZCosmos = "AZCosmos"
    GCP = "GCP"
    ECHODATAFLOW = "ECHODATAFLOW"
    EDFRUN = "EDFRUN"


class StorageOptions(BaseModel):
    """
    Model for defining storage options.

    Attributes:
        block_type (Optional[StorageType]): The type of storage. Must be one of the defined StorageType enumeration values.
        block_name (Optional[str]): The name of the block. Must be between 3 and 100 characters.
        anon (Optional[StrictBool]): Whether to use anonymous access. Default is False.
    """
    block_type: Optional[StorageType] = Field(None, description="The type of storage. Must be one of the defined StorageType enumeration values.")
    block_name: Optional[str] = Field(None, description="The name of the storage block.")
    anon: StrictBool = Field(False, description="Whether to use anonymous access. Default is False.")
    
    _cached_options: Optional[dict] = None  # Cache storage

    @property
    def _storage_options_dict(self):
        """
        Extracts storage options using the handle_storage_options function.
        
        Returns:
            dict: A dictionary representation of the storage options.
        """
        if self._cached_options is None:
            from echodataflow.utils.filesystem_utils import handle_storage_options
            self._cached_options = handle_storage_options(self)
        return self._cached_options

    # Model-wide validator to ensure logical dependencies between fields
    @model_validator(mode='before')
    def validate_storage_options(cls, values):
        storage_type = values.get("block_type")
        block_name = values.get("block_name")

        if storage_type is None and block_name:
            raise ValueError("block_name cannot be set if type is None.")

        if storage_type and not block_name:
            raise ValueError(f"A block_name must be provided when storage type is set to '{storage_type}'.")

        return values
    
    class Config:        
        use_enum_values = True

    def __repr__(self):
        return f"StorageOptions(type={self.block_type}, block_name={self.block_name}, anon={self.anon})"

