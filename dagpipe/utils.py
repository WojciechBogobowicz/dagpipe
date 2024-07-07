import inspect


class ArgumentError(Exception):
    """Custom exception for argument errors."""
    

class EmptyInputError(Exception):
    """Custom exception for empty input errors."""


# def uniform_args_kwargs_order(func, input_args, input_kwargs, allow_empty_input=True):
#     """
#     Reorders input_args and input_kwargs to match the signature of the given function.

#     Args:
#         func (callable): The function whose signature is to be matched.
#         input_args (list): List of positional arguments.
#         input_kwargs (dict): Dictionary of keyword arguments.
#         allow_empty_input (bool): Whether to allow inspect._empty objects in the output. 
#                                   If False, raises an error when inspect._empty objects are encountered.

#     Returns:
#         tuple: A tuple containing reordered positional arguments (output_args) and keyword arguments (output_kwargs).

#     Raises:
#         ArgumentError: If an argument is passed both as a positional and a keyword argument.
#         EmptyInputError: If allow_empty_input is False and inspect._empty objects are encountered.
#     """
#     # Get the signature of the function
#     sig = inspect.signature(func)
#     params = sig.parameters
    
#     output_args = []
#     output_kwargs = {}
    
#     # Keep track of the index in input_args
#     arg_index = 0

#     # Track which parameters have been used to detect duplicates
#     used_params = set()
    
#     for param_name, param in params.items():
#         if param.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD):
#             if arg_index < len(input_args):
#                 if param_name in input_kwargs:
#                     raise ArgumentError(f"Argument '{param_name}' is passed both as a positional and a keyword argument.")
#                 output_args.append(input_args[arg_index])
#                 used_params.add(param_name)
#                 arg_index += 1
#             elif param_name in input_kwargs:
#                 output_args.append(input_kwargs.pop(param_name))
#                 used_params.add(param_name)
#             elif param.default is not inspect.Parameter.empty:
#                 if not allow_empty_input:
#                     raise EmptyInputError(f"Argument '{param_name}' is required but not provided.")
#             elif not allow_empty_input:
#                 raise EmptyInputError(f"Argument '{param_name}' is required but not provided.")
#         elif param.kind == inspect.Parameter.VAR_POSITIONAL:
#             output_args.extend(input_args[arg_index:])
#             arg_index = len(input_args)
#         elif param.kind == inspect.Parameter.KEYWORD_ONLY:
#             if param_name in input_kwargs:
#                 output_kwargs[param_name] = input_kwargs.pop(param_name)
#                 used_params.add(param_name)
#             elif param.default is not inspect.Parameter.empty:
#                 if not allow_empty_input:
#                     raise EmptyInputError(f"Keyword argument '{param_name}' is required but not provided.")
#             elif not allow_empty_input:
#                 raise EmptyInputError(f"Keyword argument '{param_name}' is required but not provided.")
#         elif param.kind == inspect.Parameter.VAR_KEYWORD:
#             output_kwargs.update(input_kwargs)
#             input_kwargs = {}
    
#     # Raise an error if any input_kwargs are also in used_params
#     for kwarg in input_kwargs:
#         if kwarg in used_params:
#             raise ArgumentError(f"Argument '{kwarg}' is passed both as a positional and a keyword argument.")
    
#     # Any remaining input_kwargs go to output_kwargs if **kwargs is in the signature
#     output_kwargs.update(input_kwargs)
    
#     return tuple(output_args), output_kwargs



def uniform_args_kwargs_order(func, input_args, input_kwargs, allow_empty_input=True):
    """Reorders input_args and input_kwargs to match the signature of the given function.

    Args:
        func (callable): The function whose signature is to be matched.
        input_args (list): List of positional arguments.
        input_kwargs (dict): Dictionary of keyword arguments.
        allow_empty_input (bool): Whether to allow inspect._empty objects in the output. 
                                  If False, raises an error when inspect._empty objects are encountered.

    Returns:
        tuple: A tuple containing reordered positional arguments (output_args) and keyword arguments (output_kwargs).

    Raises:
        ArgumentError: If an argument is passed both as a positional and a keyword argument.
        EmptyInputError: If allow_empty_input is False and inspect._empty objects are encountered.
    """
    _, params = __get_function_signature(func)
    output_args, output_kwargs, arg_index, used_params = __initialize_variables()

    arg_index = __process_positional_arguments(params, input_args, input_kwargs, allow_empty_input, output_args, used_params, arg_index)
    arg_index = __process_var_positional_arguments(params, input_args, output_args, arg_index)
    output_kwargs = __process_keyword_arguments(params, input_kwargs, allow_empty_input, output_kwargs, used_params)
    output_kwargs = __process_var_keyword_arguments(params, input_kwargs, output_kwargs)
    input_kwargs = __check_remaining_kwargs(input_kwargs, used_params)

    output_kwargs.update(input_kwargs)

    return tuple(output_args), output_kwargs



def __get_function_signature(func):
    return inspect.signature(func), inspect.signature(func).parameters


def __initialize_variables():
    return [], {}, 0, set()


def __process_positional_arguments(params, input_args, input_kwargs, allow_empty_input, output_args, used_params, arg_index):
    for param_name, param in params.items():
        if param.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD):
            arg_index = __handle_positional_or_keyword(param_name, param, input_args, input_kwargs, allow_empty_input, output_args, used_params, arg_index)
    return arg_index

def __handle_positional_or_keyword(param_name, param, input_args, input_kwargs, allow_empty_input, output_args, used_params, arg_index):
    if arg_index < len(input_args):
        if param_name in input_kwargs:
            raise ArgumentError(f"Argument '{param_name}' is passed both as a positional and a keyword argument.")
        output_args.append(input_args[arg_index])
        used_params.add(param_name)
        arg_index += 1
    elif param_name in input_kwargs:
        output_args.append(input_kwargs.pop(param_name))
        used_params.add(param_name)
    elif param.default is not inspect.Parameter.empty:
        if not allow_empty_input:
            raise EmptyInputError(f"Argument '{param_name}' is required but not provided.")
    elif not allow_empty_input:
        raise EmptyInputError(f"Argument '{param_name}' is required but not provided.")
    return arg_index

def __process_var_positional_arguments(params, input_args, output_args, arg_index):
    for _, param in params.items():
        if param.kind == inspect.Parameter.VAR_POSITIONAL:
            output_args.extend(input_args[arg_index:])
            arg_index = len(input_args)
    return arg_index

def __process_keyword_arguments(params, input_kwargs, allow_empty_input, output_kwargs, used_params):
    for param_name, param in params.items():
        if param.kind == inspect.Parameter.KEYWORD_ONLY:
            __handle_keyword_only(param_name, param, input_kwargs, allow_empty_input, output_kwargs, used_params)
    return output_kwargs

def __handle_keyword_only(param_name, param, input_kwargs, allow_empty_input, output_kwargs, used_params):
    if param_name in input_kwargs:
        output_kwargs[param_name] = input_kwargs.pop(param_name)
        used_params.add(param_name)
    elif param.default is not inspect.Parameter.empty:
        if not allow_empty_input:
            raise EmptyInputError(f"Keyword argument '{param_name}' is required but not provided.")
    elif not allow_empty_input:
        raise EmptyInputError(f"Keyword argument '{param_name}' is required but not provided.")

def __process_var_keyword_arguments(params, input_kwargs, output_kwargs):
    for _, param in params.items():
        if param.kind == inspect.Parameter.VAR_KEYWORD:
            output_kwargs.update(input_kwargs)
            input_kwargs.clear()
    return output_kwargs

def __check_remaining_kwargs(input_kwargs, used_params):
    for kwarg in input_kwargs:
        if kwarg in used_params:
            raise ArgumentError(f"Argument '{kwarg}' is passed both as a positional and a keyword argument.")
    return input_kwargs


