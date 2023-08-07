import functools
import logging
from echoflow.stages_v2.aspects.singleton_echoflow import Singleton_Echoflow
from echoflow.stages_v2.utils.rest_utils import get_last_run_history


def echoflow(processing_stage: str = "DEFAULT", type: str = "TASK"):
    def decorator(func=None):
        def before_function_call(
            gea: Singleton_Echoflow, type: str, processing_stage: str, *args, **kwargs
        ):
            mod_args = []
            if type == "FLOW" and processing_stage != "DEFAULT":
                run_history = get_last_run_history(name=processing_stage)
                mod_args = [arg for arg in args]
                # gea.log(
                #     msg=run_history,
                #     extra={"mod_name": func.__module__, "func_name": func.__name__},
                #     level=logging.DEBUG,
                # )
            else:
                mod_args = [arg for arg in args]
            gea.log(
                msg=f"Entering with memory at {gea.log_memory_usage()}: ",
                extra={"mod_name": func.__module__, "func_name": func.__name__},
                level=logging.DEBUG,
            )

            return mod_args

        def after_function_call(gea: Singleton_Echoflow, *args, **kwargs):
            gea.log(
                msg=f"Exiting with memory at {gea.log_memory_usage()}: ",
                extra={"mod_name": func.__module__, "func_name": func.__name__},
                level=logging.DEBUG,
            )

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            gea = Singleton_Echoflow.get_instance()
            # process = Process(name=func.__name__)
            try:
                mod_args = before_function_call(gea, type, processing_stage, *args, **kwargs)

                result = func(*mod_args, **kwargs)
                after_function_call(gea, *args, **kwargs)
                return result
            except Exception as e:
                gea.log(
                    msg=e,
                    level=logging.ERROR,
                    extra={"mod_name": func.__module__, "func_name": func.__name__},
                )
                # process.error = e
                # gea.db_log.error = e
                # gea.db_log.status = False
                raise e
            finally:
                if processing_stage != "DEFAULT":
                    print("")
                    # gea.add_new_process(process=process, name=processing_stage)
                    # id = gea.insert_log_data()
                    # gea.db_log.run_id = id

        return wrapper

    return decorator
