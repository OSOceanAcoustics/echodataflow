"""
This module provides utility functions for dynamic function invocation and retrieving function arguments.

Module Functions:
    dynamic_function_call(module_name: str, function_name: str) -> Optional[Callable]:
        Dynamically calls a function from a specified module.

    get_function_arguments(function: Callable) -> List[str]:
        Retrieves the argument names of a given function.

Author: Soham Butala
Email: sbutala@uw.edu
Date: August 22, 2023
"""
import importlib
import inspect


def dynamic_function_call(module_name, function_name):
    """
    Dynamically calls a function from a specified module.

    Args:
        module_name (str): Name of the module.
        function_name (str): Name of the function to call.

    Returns:
        Callable or None: The function object if found, otherwise None.

    Example:
        function = dynamic_function_call('my_module', 'my_function')
        if function:
            result = function()
    """
    try:
        module = importlib.import_module(module_name)
        function = getattr(module, function_name)
        return function
    except ImportError:
        print(f"Module '{module_name}' not found.")
    except AttributeError:
        print(f"Function '{function_name}' not found in module '{module_name}'.")


def get_function_arguments(function):
    """
    Retrieves the argument names of a given function.

    Args:
        function (Callable): The function object.

    Returns:
        List[str]: List of argument names.

    Example:
        def my_function(arg1, arg2):
            pass
        arguments = get_function_arguments(my_function)
        # Result: ['arg1', 'arg2']
    """
    signature = inspect.signature(function)
    parameters = signature.parameters
    argument_names = [param.name for param in parameters.values()]
    return argument_names
