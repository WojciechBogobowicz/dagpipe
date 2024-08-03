"""
This module provides decorators for wrapping functions and methods 
in Task and MethodTask instances. After wrapping function execution would be
postponed until 'Task.run' method would not be called.

Functions:
    task(func): A decorator that wraps a function in a Task instance.
    method_task(method): A decorator that wraps a method in a MethodTask instance.
"""

import functools
from typing import Any, Callable

from dagpipe.task_core import Task, MethodTask
from dagpipe.typing import MethodTaskDecoratorType, TaskDecoratorType


def task(func=None, name="auto", outputs_num: int = 1) -> TaskDecoratorType:
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

    def decorator(func) -> Callable[[Any], Task]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Task:
            return Task(
                func,
                *args,
                name=name,
                outputs_num=outputs_num,
                **kwargs)
        return wrapper

    if func:
        return decorator(func)
    return decorator


def method_task(method=None, name="auto", outputs_num: int = 1
                ) -> MethodTaskDecoratorType:
    """
    Similar to 'task' but works with class method
    instead of standalone functions.

    Args:
        name (str, optional): Name used in visualization.
        outputs_num (int, optional): Number of of outputs, function returns.
    Returns:
        callable: A wrapped method that returns a MethodTask instance.
    """
    def decorator(method) -> Callable[[Any], Task]:
        @functools.wraps(method)
        def wrapper(instance, *args, **kwargs) -> Task:
            return MethodTask(
                instance,
                method,
                *args,
                name=name,
                outputs_num=outputs_num,
                **kwargs)
        return wrapper

    if method:
        return decorator(method)
    return decorator
