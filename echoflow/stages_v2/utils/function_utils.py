import importlib
import inspect

def dynamic_function_call(module_name, function_name):
    try:
        module = importlib.import_module(module_name)
        function = getattr(module, function_name)
        return function
    except ImportError:
        print(f"Module '{module_name}' not found.")
    except AttributeError:
        print(f"Function '{function_name}' not found in module '{module_name}'.")


def get_function_arguments(function):
    signature = inspect.signature(function)
    parameters = signature.parameters
    argument_names = [param.name for param in parameters.values()]
    return argument_names