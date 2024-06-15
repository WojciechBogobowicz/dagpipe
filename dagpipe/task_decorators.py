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

from dagpipe.task_core import Task, MethodTask


def task(func) -> callable:
    """
    A decorator that wraps a function in a Task instance.
    Task postpone function execution, 
    to the moment when its 'run' method is called.

    Args:
        func (callable): The function to be wrapped.

    Returns:
        callable: A wrapped function that returns a Task instance.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return Task(func, *args, **kwargs)
    return wrapper


def method_task(method) -> callable:
    """
    Similar to 'task' but works with class method instead of standalone functions. 

    Args:
        method (callable): The method to be wrapped.

    Returns:
        callable: A wrapped method that returns a MethodTask instance.
    """
    @functools.wraps(method)
    def wrapper(instance, *args, **kwargs):
        return MethodTask(instance, method, *args, **kwargs)
    return wrapper
