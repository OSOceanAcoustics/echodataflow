"""
Module containing a decorator for logging and aspect-oriented programming in echodataflow.

This module provides a decorator called `echodataflow` which is used for logging and
aspect-oriented programming in the echodataflow framework.

Functions:
    echodataflow(processing_stage: str = "DEFAULT", type: str = "TASK"):
        A decorator used to log function entry and exit, as well as modify arguments
        based on the execution context.

Author: Soham Butala
Email: sbutala@uw.edu
Date: August 22, 2023
"""
import asyncio
import functools
import logging
from typing import Coroutine

from distributed import get_client

from .singleton_echodataflow import Singleton_Echodataflow


def echodataflow(processing_stage: str = "DEFAULT", type: str = "TASK"):
    """
    Decorator for logging and aspect-oriented programming in the echodataflow framework.

    This decorator is used to log the entry and exit of a decorated function, as well as
    modify the function's arguments based on the execution context. It supports two types
    of execution: "TASK" and "FLOW". For a "FLOW" execution, if a processing stage other
    than the default is provided, it fetches the history of the last run for that stage
    and modifies the arguments accordingly.

    Args:
        processing_stage (str, optional): The processing stage identifier. Defaults to "DEFAULT".
        type (str, optional): The type of execution, either "TASK" or "FLOW". Defaults to "TASK".

    Returns:
        function: The decorated function.

    Example:
        @echodataflow(processing_stage="StageA", type="FLOW")
        def my_function(arg1, arg2):
            # Function code here
            pass
    """
    def decorator(func=None):
        def before_function_call(
            gea: Singleton_Echodataflow, type: str, processing_stage: str, *args, **kwargs
        ):
                
            if gea:
                gea.log(
                    msg=f"Entering with memory at {gea.log_memory_usage()}: ",
                    extra={"mod_name": func.__module__, "func_name": func.__name__},
                    level=logging.DEBUG,
                )
            
            # Deprecating, since we have check before starting the pipeline

            # if type == "FLOW" and processing_stage!= "DEFAULT":
            #     prev_stage = args[-1]
            #     stage = args[-2]
            #     if prev_stage is not None:
            #         possible_functions = gea.get_possible_next_functions(prev_stage.name)
            #         if stage.name not in possible_functions:
            #             raise ValueError(stage.name, " cannot be executed after ", prev_stage.name, ". Please consider configuring rules if this validation is wrong.")

        def after_function_call(gea: Singleton_Echodataflow, *args, **kwargs):
            if gea:
                gea.log(
                    msg=f"Exiting with memory at {gea.log_memory_usage()}: ",
                    extra={"mod_name": func.__module__, "func_name": func.__name__},
                    level=logging.DEBUG,
                )

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            gea = Singleton_Echodataflow.get_instance()
            try:
                before_function_call(gea, type, processing_stage, *args, **kwargs)
                result = func(*args, **kwargs)
                after_function_call(gea, *args, **kwargs)
                return result
            except Exception as e:
                raise e

        return wrapper

    return decorator
