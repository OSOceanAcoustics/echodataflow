from typing import Any, Dict, Optional, Union

import nest_asyncio
from prefect.filesystems import Block
from prefect_aws import AwsCredentials
from prefect_azure import AzureCosmosDbCredentials

from echodataflow.models.datastore import StorageOptions, StorageType
from echodataflow.models.echodataflow_config import (BaseConfig,
                                                     EchodataflowConfig)
from echodataflow.models.run import EDFRun


def handle_storage_options(storage_options: Optional[Union[Dict, StorageOptions, Block, BaseConfig]] = None) -> Dict:
    if isinstance(storage_options, Block):
        return _handle_block(storage_options)
    elif isinstance(storage_options, dict):
        return _handle_dict_options(storage_options)
    elif isinstance(storage_options, StorageOptions):
        return _handle_storage_options_class(storage_options)
    elif isinstance(storage_options, BaseConfig):
        return _handle_baseconfig_options_class(storage_options)
    else:
        return _handle_default(storage_options)

def _handle_block(block: Block) -> Dict:
    return get_storage_options(storage_options=block)

def _handle_dict_options(options: Dict[str, Any]) -> Dict:
    if "block_name" in options:
        block = load_block(name=options["block_name"], type=options.get("type", None))
        return get_storage_options(block)
    return options if options else {}

def _handle_storage_options_class(options: StorageOptions) -> Dict:
    if not options.anon:
        block = load_block(name=options.block_name, type=options.type)
        return get_storage_options(block)
    return {"anon": options.anon}

def _handle_baseconfig_options_class(options: BaseConfig) -> Dict:    
    block = load_block(name=options.name, type=options.type)
    return dict(block)

def _handle_default(options: Dict[str, Any]):
    return options if options else {}


def get_storage_options(storage_options: Block = None) -> Dict[str, Any]:
    """
    Get storage options from a Block.

    Parameters:
        storage_options (Block, optional): A block containing storage options.

    Returns:
        Dict[str, Any]: Dictionary containing storage options.

    Example:
        aws_credentials = AwsCredentials(...)
        storage_opts = get_storage_options(aws_credentials)
    """
    storage_options_dict: Dict[str, Any] = {}
    if storage_options is not None:
        if isinstance(storage_options, AwsCredentials):
            storage_options_dict["key"] = storage_options.aws_access_key_id
            storage_options_dict[
                "secret"
            ] = storage_options.aws_secret_access_key.get_secret_value()
            if storage_options.aws_session_token:
                storage_options_dict["token"] = storage_options.aws_session_token

    return storage_options_dict

def load_block(name: str, stype: StorageType):
    """
    Load a block of a specific type by name.

    Parameters:
        name (str, optional): The name of the block to load.
        type (StorageType, optional): The type of the block to load.

    Returns:
        block: The loaded block.

    Raises:
        ValueError: If name or type is not provided.

    Example:
        loaded_aws_credentials = load_block(name="my-aws-creds", type=StorageType.AWS)
    """
    if name is None or stype is None:
        raise ValueError("Cannot load block without name or type")
    
    loader_map = {
        StorageType.AWS: AwsCredentials,
        StorageType.AZCosmos: AzureCosmosDbCredentials,
        StorageType.ECHODATAFLOW: EchodataflowConfig,
        StorageType.EDFRUN: EDFRun
    }
    
    if stype in loader_map:
        return nest_asyncio.asyncio.run(loader_map[stype].load(name=name))
    else:
        raise ValueError(f"Unsupported storage type: {stype}")