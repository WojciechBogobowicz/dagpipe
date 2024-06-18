"""
This module provides decorators for wrapping functions and methods 
in Task and MethodTask instances. After wrapping function execution would be
postponed until 'Task.run' method would not be called.

Functions:
    task(func): A decorator that wraps a function in a Task instance.
    method_task(method): A decorator that wraps a method in a MethodTask instance.
"""

import functools
import inspect
from typing import Any, Callable

from dagpipe.task_core import Task, MethodTask


def task(name="auto", outputs_num: int = 1) -> Callable[[Callable], Callable[[Any], Task]]:
    """
    A decorator that wraps a function in a Task instance.
    Task postpone function execution, 
    to the moment when its 'run' method is called.

    Args:
        name (str, optional): Name used in visualization.
        outputs_num (int, optional): Number of of outputs, function returns. 

    Returns:
        callable: A wrapped function that returns a Task instance.
    """
    def decorator(func):
        # @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Task:
            return Task(func, *args, name=name, outputs_num=outputs_num, **kwargs)
        return wrapper
    return decorator


def method_task(name="auto", outputs_num=1) -> Callable[[Callable], Callable[[Any], MethodTask]]:
    """
    Similar to 'task' but works with class method instead of standalone functions. 

    Args:
        name (str, optional): Name used in visualization.
        outputs_num (int, optional): Number of of outputs, function returns.
    Returns:
        callable: A wrapped method that returns a MethodTask instance.
    """
    def decorator(method):
        # @functools.wraps(method)
        def wrapper(instance, *args, **kwargs) -> Task:
            return MethodTask(instance, method, *args, name=name, outputs_num=outputs_num, **kwargs)
        return wrapper
    return decorator
