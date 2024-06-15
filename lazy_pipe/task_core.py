"""
This module defines the Task and MethodTask classes used to represent and manage tasks.

Classes:
    Task: A class representing a task that postpone function execution
    with given parameters.
    MethodTask: Similar to task, but works with class methods.
"""

from typing import Any


class Task:
    """
    A class representing a task that can be executed with given arguments.
    It is used to wraps output if function decorated with task decorator,
    and delay this function execution.

    Attributes:
        func (callable): The function to be executed.
        args (tuple): The positional arguments to be passed to the function.
        kwargs (dict): The keyword arguments to be passed to the function.
        evaluated_result (Any): The result of the function execution.
    """
    def __init__(self, func, *args, **kwargs):
        """
        Initialize a Task instance.

        Args:
            func (callable): The function to be executed.
            *args: Positional arguments to be passed to the function.
            **kwargs: Keyword arguments to be passed to the function.
        """
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.evaluated_result = None

    def run(self, *args, **kwargs) -> Any:
        """
        Execute the task with provided arguments and update the evaluated result.

        Args:
            *args: Positional arguments to override currently stored arguments.
            **kwargs: Keyword arguments to override currently stored arguments.

        Returns:
            Any: The result of the function execution.
        """
        self.update_args_if_provided(*args, **kwargs)
        args, kwargs = self.unpack_args_from_results()
        self.evaluated_result = self.evaluate_result(*args, **kwargs)
        return self.evaluated_result

    def evaluate_result(self, *args, **kwargs) -> Any:
        """
        Evaluate the result by executing the function with given arguments.
        """
        return self.func(*args, **kwargs)

    def update_args_if_provided(self, *args, **kwargs) -> None:
        """
        Update the initial arguments if new arguments are provided.

        Args:
            *args: Totally replace currently stored args.
            **kwargs: Only update existing kwargs.
        """
        if args or kwargs:
            self.args = args if args else self.args
            self.kwargs.update(kwargs)

    def unpack_args_from_results(self) -> tuple[tuple, dict]:
        """
        Process self.args and self.kwargs in a way that results
        are encapsulated from Task type values, and rest remains unchanged.

        Returns:
            Tuple[Tuple, Dict]: The unpacked positional and keyword arguments.
        """
        args = tuple(a if not isinstance(a, Task) else a.evaluated_result for a in self.args)
        kwargs = {k: v if not isinstance(v, Task) else v.evaluated_result for k, v in self.kwargs.items()}
        return args, kwargs

    def __repr__(self) -> str:
        """
        Return a string representation of the Task instance.

        Returns:
            str: The name of the inner function.
        """
        return self.func.__name__

class MethodTask(Task):
    """
    A class representing a task that involves executing a method on an instance.

    Attributes:
        instance (Any): The instance on which the method will be executed.
    """

    def __init__(self, instance, func, *args, **kwargs):
        """
        Initialize a MethodTask instance.

        Args:
            instance (Any): The instance on which the method will be executed.
            func (callable): The method to be executed.
            *args: Positional arguments to be passed to the method.
            **kwargs: Keyword arguments to be passed to the method.
        """
        super().__init__(func, *args, **kwargs)
        self.instance = instance

    def evaluate_result(self, *args, **kwargs) -> Any:
        """
        Evaluate the result by executing the method with given arguments.
        """
        return self.func(self.instance, *args, **kwargs)

    def __repr__(self) -> str:
        """
        Return a string representation of the MethodTask instance in form:
        - 'ClassName.function_name' in most cases
        - 'ClassName' if __call__ method is wrapped.
        Returns:
            str: The name of the instance and the method.
        """
        if self.func.__name__ == "__call__":
            return self.instance.__class__.__name__
        return f"{self.instance.__class__.__name__}.{self.func.__name__}"
