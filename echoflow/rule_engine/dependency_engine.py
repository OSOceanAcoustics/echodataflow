from collections import defaultdict


class DependencyEngine:
    """
    Dependency Engine for Managing Function Dependencies

    This class represents a dependency engine that helps manage dependencies between
    functions. It allows adding dependencies between target functions and dependent
    functions. The engine then provides a mechanism to retrieve the functions that can
    potentially be executed next, given a current function's completion.

    Usage:
        Initialize an instance of the DependencyEngine class. Use the `add_dependency`
        method to establish dependencies between functions. The `get_possible_next_functions`
        method can be used to retrieve the functions that can be executed next, based on
        the completion of a given function.

    Example:
        # Initialize the DependencyEngine
        engine = DependencyEngine()

        # Add dependencies
        engine.add_dependency(target_function="download_data", dependent_function="preprocess_data")
        engine.add_dependency(target_function="preprocess_data", dependent_function="analyze_data")

        # Retrieve possible next functions after "download_data"
        next_functions = engine.get_possible_next_functions(current_function="download_data")
    """
    def __init__(self):
        self.dependencies = defaultdict(list)

    def add_dependency(self, target_function, dependent_function):
        """
        Add a Dependency between Functions

        This method adds a dependency relationship between a target function and a
        dependent function. The dependent function is expected to be executed after
        the successful completion of the target function.

        Args:
            target_function (str): The name of the target function.
            dependent_function (str): The name of the dependent function.

        Example:
            # Add a dependency relationship
            engine.add_dependency(target_function="data_download", dependent_function="data_preprocess")
        """        
        self.dependencies[target_function].append(dependent_function)

    def get_possible_next_functions(self, current_function):
        """
        Get Possible Next Functions

        Retrieve a list of functions that can potentially be executed next, given the
        completion of a current function.

        Args:
            current_function (str): The name of the function that has completed.

        Returns:
            list: A list of functions that can be executed next.

        Example:
            # Get possible next functions after "data_download"
            next_functions = engine.get_possible_next_functions(current_function="data_download")
        """
        if current_function in self.dependencies:
            return self.dependencies[current_function]
        else:
            return []
    
    