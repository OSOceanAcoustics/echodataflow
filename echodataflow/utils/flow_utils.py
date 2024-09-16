from typing import Dict
from echodataflow.models.datastore import Dataset
from echodataflow.models.output_model import Group
from echodataflow.models.pipeline import Stage
from echodataflow.utils import log_util
from pathlib import Path
import torch
from echodataflow.utils.xr_utils import fetch_slice_from_store
from src.model.BinaryHakeModel import BinaryHakeModel


def load_model(stage: Stage, config: Dataset):
    try:
        log_util.log(
            msg={"msg": f"Loading model now ---->", "mod_name": __file__, "func_name": "Mask_Prediction"},
            use_dask=stage.options["use_dask"],
            eflogging=config.logging,
        )
        model_path = f"/home/exouser/hake_data/model/backup_model_weights/binary_hake_model_1.0m_bottom_offset_1.0m_depth_2017_2019_ver_1.ckpt"
            
        # Load binary hake models with weights
        model = BinaryHakeModel("placeholder_experiment_name",
                                Path("placeholder_score_tensor_dir"),
                                "placeholder_tensor_log_dir", 0).eval()

        model.load_state_dict(torch.load(
            stage.external_params.get('model_path', model_path)
            )["state_dict"])
        
        log_util.log(
            msg={"msg": f"Model loaded succefully", "mod_name": __file__, "func_name": "Mask_Prediction"},
            use_dask=stage.options["use_dask"],
            eflogging=config.logging,
        )
    except Exception as e:
        log_util.log(
            msg={"msg": "", "mod_name": __file__, "func_name": "Mask_Prediction"},
            use_dask=stage.options["use_dask"],
            eflogging=config.logging,
            error=e
        )
        raise e
    
    return model
    
def load_data_in_memory(config: Dataset, groups: Dict[str, Group]):
    
    for _, gr in groups.items():
        # From a store (list of file paths) fetch the slice of data and keep it in memory
        if gr.metadata and gr.metadata.is_store_folder and len(gr.data) > 0:
                edf = gr.data[0]
                edf.data = fetch_slice_from_store(edf_group=gr, config=config, start_time=edf.start_time, end_time=edf.end_time)
                if edf.data.notnull().any():
                    gr.data = [edf]
                    gr.metadata.is_store_folder = False
                else:
                    continue
    return groups