"""
This module defines the Task and MethodTask classes used to represent and manage tasks.

Classes:
    Task: A class representing a task that postpone function execution
    with given parameters.
    MethodTask: Similar to task, but works with class methods.
"""

from typing import Any, Iterable


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
        name (str): The name of the task.
        outputs_num (int): The number of outputs the function returns.
        references (list): List of TaskReference objects.

    """
    def __init__(self, func, *args, name="auto", outputs_num=1, **kwargs):
        """
        Initialize a Task instance.

        Args:
            func (callable): The function to be executed.
            name (str, optional): Name that would be displayed in visualization.
            outputs_num (int, optional): Number of of outputs, function returns.
            *args: Positional arguments to be passed to the function.
            **kwargs: Keyword arguments to be passed to the function.
        """
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.name = name if name != "auto" else repr(self)
        self.evaluated_result = None
        self.outputs_num=outputs_num
        self.references = None
        self._index = 0

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
            *args: Replace currently stored args starting from args beginning.
            **kwargs: Only update existing kwargs.
        """
        if args:
            self.args = tuple([*args, *self.args[len(args):]])
        if kwargs:
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
    
    def __iter__(self):
        """
        Initialize an iterator for the task's outputs.
        """
        if self.references is None:
            self.references = []
            for index in range(self.outputs_num):
                ref_name = f"{self.name}[{index}]"
                self.references.append(TaskReference(self, index, ref_name))
        self._index = 0
        return self
    
    def __next__(self):
        """
        Get the next output reference in the iteration.

        Returns:
            TaskReference: The next TaskReference object.

        Raises:
            StopIteration: If there are no more outputs to iterate over.
        """
        if self._index == self.outputs_num:
            raise StopIteration
        self._index += 1
        return self.references[self._index-1]

    def set_name(self, name: str | Iterable):
        """
        Set the name of the task or its output references.

        Args:
            name (str or Iterable): The new name for the task or a list of names for the outputs.

        Returns:
            self: The Task instance itself.
        """
        if isinstance(name, str):
            self.name = name
        else:
            for ref, name in zip(self, name):
                ref.name = name
        return self
    
    def to_stopping_holder(self):
        """Returns holder for task."""
        return StoppingTaskHolder(self)
        

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

    def __init__(self, instance, func, *args, name="auto", outputs_num=1, **kwargs):
        """
        Initialize a MethodTask instance.

        Args:
            instance (Any): The instance on which the method will be executed.
            func (callable): The method to be executed.
            name (str, optional): Name that would be displayed in visualization.
            outputs_num (int, optional): Number of of outputs, function returns.
            *args: Positional arguments to be passed to the method.
            **kwargs: Keyword arguments to be passed to the method.
        """
        self.instance = instance
        super().__init__(func, *args, name=name, outputs_num=outputs_num, **kwargs)

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


class TaskReference(Task):
    """
    A class representing a reference to a specific output of a Task.

    Attributes:
        task (Task): The original task this reference points to.
        ref_index (int): The index of the output this reference points to.
    """
    def __init__(self, task: Task, ref_index: int, name: str):
        """
        Initialize a TaskReference instance.

        Args:
            task (Task): The original task this reference points to.
            ref_index (int): The index of the output this reference points to.
            name (str): The name of the reference.
        """
        super().__init__(task.func, *task.args, name=name, outputs_num=1, **task.kwargs)
        self.task = task
        self.ref_index = ref_index
        
    def run(self, *args, **kwargs) -> Any:
        """
        Execute the reference task if necessary and update the evaluated result.

        Args:
            *args: Positional arguments to override currently stored arguments.
            **kwargs: Keyword arguments to override currently stored arguments.

        Returns:
            Any: The result of the function execution.
        """
        if self._all_refs_ran() or (not self.task.evaluated_result):
            self.task.run(*args, **kwargs)
        self.evaluated_result = self.task.evaluated_result[self.ref_index]
        return self.evaluated_result

    def _all_refs_ran(self) -> bool:
        """
        Check if all references have run and update the reference tracking.

        Returns:
            bool: True if all references have run, False otherwise.
        """
        if not hasattr(self.task, "ran_refs"):
            self.task.ran_refs = [self]
            return True
        if self in self.task.ran_refs:
            self.task.ran_refs = [self]
            return True
        else:
            self.task.ran_refs.append(self)
            return False

    def __repr__(self) -> str:
        """
        Return a string representation of the Task Reference instance.

        Returns:
            str: *task*-ref[*id*].
        """
        return f"{repr(self.task)}-ref[{self.ref_index}]"


class StoppingTaskHolder:
    """
    A class that wraps a task to indicate that it has been stopped.

    Attributes:
        task (Task): The task that is being held and marked as stopped.
    """
    def __init__(self, task: Task):
        """
        Initialize a StoppingTaskHolder instance.

        Args:
            task (Task): The task to be held and marked as stopped.
        """
        self.task = task
    
    @classmethod
    def in_(self, collection: Iterable):
        return any([isinstance(elem, StoppingTaskHolder) for elem in collection])

    def __repr__(self) -> str:
        return "STOPPED AT " + self.task.__repr__()
