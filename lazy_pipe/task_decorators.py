import functools
import inspect

from lazy_pipe.task_core import Task, MethodTask

def task(func) -> callable:
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return Task(func, *args, **kwargs)
    return wrapper

def method_task(method) -> callable:
    @functools.wraps(method)
    def wrapper(instance, *args, **kwargs):
        return MethodTask(instance, method, *args, **kwargs)
    return wrapper

# def get_method_class(method):
#     if not (inspect.ismethod(method) or inspect.isfunction(method)):
#         raise ValueError(f"{method} is not a method or function")

#     method_self = getattr(method, '__self__', None)
#     if method_self is None:
#         raise ValueError("The method is not bound to an instance")

#     for cls in inspect.getmro(method_self.__class__):
#         if method.__name__ in cls.__dict__:
#             return cls
#     return None
