from functools import wraps
import os

from pandas_openlineage.listener import (
    get_openlineage_listener,
    DataFrameLineageMetadata,
)
import inspect


def lineage_read(path_argument_name):
    """
    Decorator for read-related IO events.

    This decorator is used to wrap functions that perform read operations,
    and emits lineage metadata events.

    Args:
        path_argument_name (str): The name of the argument in the decorated
                                  function that contains the path to the file
                                  being read.

    Returns:
        function: The wrapped function with lineage tracking.
    """

    def read_decorator(func):
        def wrapper(*args, **kwargs):
            # Set job name if not already set and emit start event
            set_job_name()
            get_openlineage_listener().attempt_to_emit_start_event()

            # Try to get the variable from kwargs
            path = kwargs.get(path_argument_name)

            # If not found in kwargs, try to get it from args
            if path is None:
                try:
                    if path_argument_name in func.__code__.co_varnames:
                        arg_index = func.__code__.co_varnames.index(path_argument_name)
                        path = args[arg_index]
                    else:
                        raise ValueError(
                            f"Argument '{path_argument_name}' not found in function arguments"
                        )
                except (ValueError, IndexError):
                    raise ValueError(
                        f"Argument '{path_argument_name}' not found in function arguments"
                    )

            result = func(*args, **kwargs)

            get_openlineage_listener().on_read(DataFrameLineageMetadata(result, path))

            return result

        return wrapper

    return read_decorator


def lineage_write(path_argument_name):
    """
    Decorator for write-related IO events.

    This decorator is used to wrap functions that perform write operations,
    and emits lineage metadata events.

    Args:
        path_argument_name (str): The name of the argument in the decorated
                                  function that contains the path to the file
                                  being written.

    Returns:
        function: The wrapped function with lineage tracking.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Try to get the variable from kwargs
            path = kwargs.get(path_argument_name)

            # If not found in kwargs, try to get it from args
            if path is None:
                try:
                    arg_index = func.__code__.co_varnames.index(path_argument_name)
                    path = args[arg_index]
                except (ValueError, IndexError):
                    raise ValueError(
                        f"Argument '{path_argument_name}' not found in function arguments"
                    )
            result = func(*args, **kwargs)
            
            dataframe = args[0]

            get_openlineage_listener().on_write(DataFrameLineageMetadata(dataframe, path))
            return result

        return wrapper

    return decorator


def set_job_name():
    """
    Sets the job name for the OpenLineage listener.

    This function checks if the job name is already set in the OpenLineage listener.
    If not, it attempts to fetch the job name from the environment variable
    'OPENLINEAGE_JOB_NAME'. If the environment variable is not set, it fetches
    the script location and sets it as the job name.
    """
    if not get_openlineage_listener().has_script_location_set:
        job_name = os.environ.get("OPENLINEAGE_JOB_NAME")
        if not job_name:
            job_name = fetch_script_location()
        get_openlineage_listener().set_script_location(job_name)


def fetch_script_location():
    """
    Fetch the location of the script that called this function.

    Returns:
        str: The file path of the caller script.
    """
    stack = inspect.stack()
    caller_frame = stack[3]  # The caller is three frames up from the current frame
    caller_file = caller_frame.filename
    return caller_file
