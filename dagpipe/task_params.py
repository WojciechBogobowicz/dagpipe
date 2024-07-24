import inspect
import importlib
from typing import Callable, Union

    

class TaskParams:
    """A class that manages and processes the parameters of a task function."""
    def __init__(self, func: Callable, *init_args, **init_kwargs) -> None:
        """
        Initialize a TaskParams instance.

        Args:
            func (Callable): The function whose parameters are managed.
            *init_args: Initial positional arguments for the function.
            **init_kwargs: Initial keyword arguments for the function.
        """
        from dagpipe.typing import TaskType
        self.__task_type = TaskType #getattr(importlib.import_module('dagpipe.typing'), 'TaskType')
        
        self._sig = inspect.signature(func)
        self._parameters = self._sig.bind(*init_args, **init_kwargs).arguments
        self._task_params_names = self._filter_tasks_from_parameters()

        self._varargs_name = self._find_param_name(inspect.Parameter.VAR_POSITIONAL)
        self._varkwargs_name = self._find_param_name(inspect.Parameter.VAR_KEYWORD)

        self._all_varargs_are_tasks = self._are_all_varargs_tasks()
        self._tasks_in_varkwargs = self._get_tasks_from_varkwargs()

    
    @property
    def evaluated_args(self) -> tuple:
        """Get the evaluated positional arguments, resolving tasks to their results."""
        # print(self.args, "<- inside evaluated_args")
        # print(tuple(a.evaluated_result if isinstance(a, self.__task_type) else a for a in self.args), "<- after evaluation")
        args = []
        args = self.__get_evaluated_results(args)
        return tuple(args)

    def __get_evaluated_results(self, args):
        for arg in self.args:
            if isinstance(arg, self.__task_type):
                args.append(arg.evaluated_result)
            # elif isinstance(arg, Union[list, tuple, set, frozenset]):
            #     args.append(self.__get_evaluated_results(arg))
            else:
                args.append(arg)
        return args
        
    @property
    def evaluated_kwargs(self) -> dict:
        """Get the evaluated keyword arguments, resolving tasks to their results."""
        return {k: v.evaluated_result 
                if isinstance(v, self.__task_type) else v
                for k, v
                in self.kwargs.items()}

    @property
    def args(self) -> tuple:
        """Get the positional arguments from the bound parameters."""
        args = []
        for param_name, param in self._sig.parameters.items():
            if param.kind in (inspect.Parameter.VAR_KEYWORD, inspect.Parameter.KEYWORD_ONLY):
                break

            if param_name in self._parameters:
                arg = self._parameters[param_name]
                if param.kind == inspect.Parameter.VAR_POSITIONAL:
                    # print(arg, "<- positional args")
                    args.extend(arg)
                else:
                    # print(arg, "<- not positional args")
                    args.append(arg)
        # print(args, "<- args from property")
        return tuple(args)

    @property
    def kwargs(self) -> dict:
        """Get the keyword arguments from the bound parameters."""
        kwargs = {}
        kwargs_started = False
        for param_name, param in self._sig.parameters.items():
            if not kwargs_started:
                if param.kind in (inspect.Parameter.VAR_KEYWORD, inspect.Parameter.KEYWORD_ONLY):
                    kwargs_started = True
                else:
                    if param_name not in self._parameters:
                        kwargs_started = True
                        continue

            if not kwargs_started:
                continue

            if param_name in self._parameters:
                arg = self._parameters[param_name]
                if param.kind == inspect.Parameter.VAR_KEYWORD:
                    kwargs.update(arg)
                else:
                    kwargs[param_name] = arg

        return kwargs

    def update(self, *args, **kwargs):
        """
        Update the bound parameters with new arguments.

        Args:
            *args: New positional arguments.
            **kwargs: New keyword arguments.
        """
        new_arguments = self._sig.bind_partial(*args, **kwargs).arguments
        self.__assert_tasks_are_not_overwritten(new_arguments)
        new_arguments = self.__update_varkwargs_with_varkwargs_tasks(new_arguments)
        for arg_name, value in new_arguments.items():
            self._parameters[arg_name] = value

    def __assert_tasks_are_not_overwritten(self, new_arguments):
        self.__assert_tasks_are_not_overwritten_in_arguments(new_arguments)
        self.__assert_tasks_are_not_overwritten_in_varargs(new_arguments)
        self.__assert_tasks_are_not_overwritten_in_varkwargs(new_arguments)

    def __assert_tasks_are_not_overwritten_in_arguments(self, new_arguments: dict):
        for name in self._task_params_names:
            if name in new_arguments:
                raise TypeError(f"Tried to overwrite {self._parameters[name]}"
                                f" with {new_arguments[name]}"
                                f" in parameter {name}."
                                " Task overwriting is not allowed.")   

    def __assert_tasks_are_not_overwritten_in_varkwargs(self, new_arguments: dict):
        if self._varkwargs_name:
            if new_varkwargs := new_arguments.get(self._varkwargs_name, None):
                for name in self._tasks_in_varkwargs:
                    if name in new_varkwargs:
                        raise TypeError(f"Tried to overwrite {self._parameters[name]}"
                                        f" with {new_arguments[name]}"
                                        f" in parameter {name}."
                                        " Task overwriting is not allowed.")        

    def __assert_tasks_are_not_overwritten_in_varargs(self, new_arguments: dict):
        if self._all_varargs_are_tasks:
            if new_arguments.get(self._varargs_name, None):
                raise TypeError("Overwriting varargs, is not allowed"
                                    " when varargs are tasks.")

    def __update_varkwargs_with_varkwargs_tasks(self, new_arguments: dict):
        if self._varkwargs_name in new_arguments:
            new_arguments[self._varkwargs_name].update(self._tasks_in_varkwargs)
        return new_arguments

    def _filter_tasks_from_parameters(self):
        return [name for name, param in self._parameters.items() if isinstance(param, self.__task_type)]

    def _are_all_varargs_tasks(self) -> bool:
        if varargs := self._parameters.get(self._varargs_name, None):
            if any(isinstance(arg, self.__task_type) for arg in varargs):
                self.__assert_all_varargs_are_tasks(varargs)
                return True
        return False

    def __assert_all_varargs_are_tasks(self, varargs):
        if not all(isinstance(arg, self.__task_type) for arg in varargs):
            raise ValueError("Either all or none varargs needs to be a TaskType.")

    def _find_param_name(self, kind):
        for name, param in self._sig.parameters.items():
            if param.kind == kind:
                return name
        return None

    def _get_tasks_from_varkwargs(self) -> dict:
        if varkwargs := self._parameters.get(self._varkwargs_name, None):
            return {k: v for k, v in varkwargs.items() if isinstance(v, self.__task_type)}
        return {}
