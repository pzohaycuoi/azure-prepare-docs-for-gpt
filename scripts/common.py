import logging
import logging.config
import os.path
import time
import inspect
from datetime import datetime


# Set default log configuration file path and log folder
cur_dir = os.path.dirname(__file__)


def convert_to_realpath(path):
    """
    Get the current workspace should be inside the report-automation folder
    Convert the path in arg to absolute path with relative path appended
    So path using relative path should be working normally
    """
    workspace_dir = os.path.dirname(
        os.path.dirname(os.path.realpath(__file__)))
    if workspace_dir not in path:
        current_path = (os.path.dirname(os.path.realpath(__file__)))
        real_path = os.path.join(current_path, path)
        return real_path
    else:
        return path


# default_log_config = cur_dir + '/../logconfig.ini'
default_log_config = convert_to_realpath("../logconfig.ini")


def logger_config(config_filepath=default_log_config):
    """
    Setup logging configuration
    """
    # check if provided file path exist
    if not os.path.exists(config_filepath):
        print(
            f"{config_filepath} not exist. Using default configs: {default_log_config}")
        logging.config.fileConfig(default_log_config)
    else:
        try:
            logging.config.fileConfig(config_filepath)
        except Exception as ex:
            print(ex)
            print(
                f"Error in Logging Configuration. Using default configs: {default_log_config}")
            logging.config.fileConfig(default_log_config)


def log_function_call(func):
    """
    Decorator for debugging
    """
    def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            file_path = inspect.getfile(func)
            file_name = os.path.basename(file_path)
            logging.debug('%s - %s - start function with args: %s', func.__name__, file_name, args)
            result = func(*args, **kwargs)
            end_time = time.time()
            logging.debug('%s - %s - %s - end function with result: %s', func.__name__, file_name,
                          end_time-start_time, result)
            return result
        except Exception as e:
            end_time = time.time()
            logging.exception('%s - %s - %s - end function with Exception: %s', func.__name__, file_name,
                              end_time-start_time, str(e))
            raise e
    return wrapper


def decor_get_real_path(func):
    """
    Decorator for function/method that return path for local
    Convert path to absolute path of the workspace
    """
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        real_path = convert_to_realpath(result)
        return real_path
    return wrapper


@log_function_call
def generate_file_name(file_path):
    """
    Genereate file name with timestamp appended,
    If arg is path, return a path with new file name
    Else if arg is a file name, return only new file name
    """
    if len(os.path.split(file_path)) > 1:
        path_parts = os.path.split(file_path)
        dir_path = path_parts[0]
        file_name = path_parts[1]
        file_name_parts = os.path.splitext(file_name)
        time_stamp = datetime.now().strftime('%d%m%y-%H%M%S')
        new_file_name = f"{file_name_parts[0]}-{time_stamp}{file_name_parts[1]}"
        new_path = os.path.join(dir_path, new_file_name)
        return new_path
    else:
        file_name_parts = os.path.splitext(file_name)
        time_stamp = datetime.now().strftime('%d%m%y-%H%M%S')
        new_file_name = f"{file_name_parts[0]}-{time_stamp}{file_name_parts[1]}"
        return new_file_name


@log_function_call
def check_path_exist(path, avoid_duplicate: bool = False):
    """
    Check path existence
    avoid_duplicate = False: Check path existence only, return bool for result
    avoid_duplicate = True: Check path existence for file path,
    generate new file path with timestamp in filename part, return new path
    """
    workspace_dir = os.path.dirname(
        os.path.dirname(os.path.realpath(__file__)))

    if workspace_dir not in path:
        real_path = convert_to_realpath(path)
    else:
        real_path = path

    if os.path.exists(real_path):
        if avoid_duplicate is True:
            new_path = generate_file_name(real_path)
            return new_path

        if avoid_duplicate is False:
            return True
    else:
        if avoid_duplicate is True:
            return real_path

        if avoid_duplicate is False:
            return False
