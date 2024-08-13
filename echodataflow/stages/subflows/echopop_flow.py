
"""
Echodataflow Echopop Integration

This module defines a Prefect Flow and associated tasks for the Echodataflow Echopop Integration.

Author: Soham Butala
Email: sbutala@uw.edu
Date: July 30, 2024
"""
import os
from typing import Dict, List, Optional

from prefect import flow, task
from echodataflow.aspects.echodataflow_aspect import echodataflow
from echodataflow.models.datastore import Dataset
from echodataflow.models.output_model import ErrorObject, Group
from echodataflow.models.pipeline import Stage
from echodataflow.utils import log_util
from echodataflow.utils.file_utils import get_working_dir

from echopop.live.live_survey import LiveSurvey

@flow
@echodataflow(processing_stage="echopop", type="FLOW")
def echopop_flow(
        groups: Dict[str, Group], config: Dataset, stage: Stage, prev_stage: Optional[Stage]
):
    working_dir = get_working_dir(stage=stage, config=config)

    futures = []
    
    for name, gr in groups.items():
        
        gname = gr.group_name + ".Echopop"
        future = live_survey_process.with_options(
            task_run_name=gname, name=gname, retries=3
        ).submit(
            gr=gr, working_dir=working_dir, config=config, stage=stage
        )
        
        futures.append(future)
         
    for f in futures:
        try:            
            res = f.result()            
            del f
            groups[res.group_name].data = res.data
        except Exception as e:
            groups[res.group_name].data[0].error = ErrorObject(errorFlag=True, error_desc=str(e))
        del res        
           
    return groups

@task
def live_survey_process(gr: Group, working_dir, config: Dataset, stage: Stage):
    
    file_name = gr.group_name + "_echopop.zarr"
    try:
        log_util.log(
            msg={"msg": " ---- Entering ----", "mod_name": __file__, "func_name": file_name},
            use_dask=stage.options["use_dask"],
            eflogging=config.logging,
        )

        # No need to load NASC anymore, just pass the paths
        # nasc = get_zarr_list.fn(
        #     transect_data=ed, storage_options=config.output.storage_options_dict
        # )[0]

        log_util.log(
            msg={"msg": f"Processing data", "mod_name": __file__, "func_name": file_name},
            use_dask=stage.options["use_dask"],
            eflogging=config.logging,
        )
        
        external_kwargs = stage.external_params
            
        realtime_survey = LiveSurvey(**external_kwargs)
        
        log_util.log(
            msg={"msg": f"Created live survey object successfully", "mod_name": __file__, "func_name": file_name},
            use_dask=stage.options["use_dask"],
            eflogging=config.logging,
        )
        
        if config.passing_params and config.passing_params["POP_TYPE"] == "BIO":
            realtime_survey.load_biology_data(input_filenames=[ed.filename+"."+ed.file_extension for ed in gr.data])
        
            log_util.log(
                msg={"msg": f"Loaded bio data successfully", "mod_name": __file__, "func_name": file_name},
                use_dask=stage.options["use_dask"],
                eflogging=config.logging,
            )
        
            realtime_survey.process_biology_data()        
        
            log_util.log(
                msg={"msg": f"Processed bio data successfully", "mod_name": __file__, "func_name": file_name},
                use_dask=stage.options["use_dask"],
                eflogging=config.logging,
            )

            realtime_survey.estimate_population(working_dataset="biology")
            
            processed_files = [str(os.path.basename(b)).split('.', maxsplit=1)[0] for b in realtime_survey.meta["provenance"]["biology_files"]]
            
            for ed in gr.data:
                if ed.filename in processed_files:
                    ed.error = ErrorObject(errorFlag=False)
                else:
                    ed.error = ErrorObject(errorFlag=True, error_desc=f"{ed.filename} was not found in the Live Survey Provenance") 
                ed.stages[stage.name] = gr.data[0].out_path
            
        else:
            realtime_survey.load_acoustic_data(input_filenames=[os.path.basename(ed.out_path) for ed in gr.data])
        
            log_util.log(
                msg={"msg": f"Loaded acoustic data successfully", "mod_name": __file__, "func_name": file_name},
                use_dask=stage.options["use_dask"],
                eflogging=config.logging,
            )
        
            realtime_survey.process_acoustic_data()        
        
            log_util.log(
                msg={"msg": f"Processed acoustic data successfully", "mod_name": __file__, "func_name": file_name},
                use_dask=stage.options["use_dask"],
                eflogging=config.logging,
            )
        
            realtime_survey.estimate_population(working_dataset="acoustic")
            
            processed_files = [str(os.path.basename(b)) for b in realtime_survey.meta["provenance"]["acoustic_files"]]

            for ed in gr.data:
                if os.path.basename(ed.out_path) in processed_files:
                    ed.error = ErrorObject(errorFlag=False)
                else:
                    ed.error = ErrorObject(errorFlag=True, error_desc=f"{ed.filename} was not found in the Live Survey Provenance") 
                ed.stages[stage.name] = gr.data[0].out_path
                
        log_util.log(
            msg={"msg": f"Estimated population for {processed_files}", "mod_name": __file__, "func_name": file_name},
            use_dask=stage.options["use_dask"],
            eflogging=config.logging,
        )

        log_util.log(
            msg={"msg": f" ---- Exiting ----", "mod_name": __file__, "func_name": file_name},
            use_dask=stage.options["use_dask"],
            eflogging=config.logging,
        )
           
    except Exception as e:
        log_util.log(
            msg={"msg": "", "mod_name": __file__, "func_name": file_name},
            use_dask=stage.options["use_dask"],
            eflogging=config.logging,
            error=e
        )
        for ed in gr.data:
            ed.error = ErrorObject(errorFlag=True, error_desc=str(e))
    finally:
        return gr