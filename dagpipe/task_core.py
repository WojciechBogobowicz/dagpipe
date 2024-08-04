"""
This module defines the Task and MethodTask classes used to represent and manage tasks.

Classes:
    Task: A class representing a task that postpone function execution
    with given parameters.
    MethodTask: Similar to task, but works with class methods.
"""

from typing import Any, Iterable, TYPE_CHECKING, Literal

from dagpipe.errors import UndefinedTaskIndexAccessError
from dagpipe.task_params import TaskParams


if TYPE_CHECKING:
    from dagpipe.pipeline import Pipeline
    # from dagpipe.typing import TaskType


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
        self._params = TaskParams(self.func, *args, **kwargs)
        self.outputs_num = outputs_num
        self.name = name if name != "auto" else self._get_function_name()
        self.evaluated_result = None
        self._ref_names: str | Literal["auto"] = "auto"

    @property
    def evaluated_args(self) -> tuple:
        return self._params.evaluated_args

    @property
    def evaluated_kwargs(self) -> dict:
        return self._params.evaluated_kwargs

    @property
    def input_tasks(self) -> list["Task"]:
        return self._params.input_tasks

    def _get_function_name(self):
        return self.func.__name__

    def run(self, *args, **kwargs) -> Any:
        """
        Execute the task with provided arguments and update the evaluated result.

        Args:
            *args: Positional arguments to override currently stored arguments.
            **kwargs: Keyword arguments to override currently stored arguments.

        Returns:
            Any: The result of the function execution.
        """
        self.update_params(*args, **kwargs)

        self.evaluated_result = self.evaluate_result(
            *self._params.evaluated_args,
            **self._params.evaluated_kwargs,
        )
        return self.evaluated_result

    def update_params(self, *args, **kwargs):
        """Update parameters that will be used when task will run."""
        self._params.update(*args, **kwargs)

    def evaluate_result(self, *args, **kwargs) -> Any:
        """
        Evaluate the result by executing the function with given arguments.
        """
        return self.func(*args, **kwargs)

    def __iter__(self):
        return TaskIterator(self)

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
            self._ref_names = name
        return self

    def split_output(self, outputs_names: Iterable[str]):
        """Create Task references with given names."""
        outputs_names = list(outputs_names)
        self.set_outputs_num(len(outputs_names))
        self.set_name(outputs_names)
        return self

    def set_outputs_num(self, num: int):
        """Redefine number of outputs in task"""
        self.outputs_num = num
        return self

    def to_stopping_holder(self):
        """Returns holder for task."""
        return StoppingTaskHolder(self)

    def _get_ref_name(self, ref_idx: int) -> str:
        self.__assert_ref_name_defined(ref_idx)

        if self._ref_names == "auto":
            ref_name = f"{self.name}[{ref_idx}]"
        else:
            ref_name = self._ref_names[ref_idx]
        return ref_name

    def __assert_ref_name_defined(self, ref_idx):
        idx_is_undefined = False
        if self._ref_names != "auto":
            if ref_idx >= len(self._ref_names):
                idx_is_undefined = True
        else:
            if ref_idx >= self.outputs_num:
                idx_is_undefined = True

        if idx_is_undefined:
            raise UndefinedTaskIndexAccessError(
                f"You are trying to access {ref_idx} element "
                f"of the {repr(self)} output. "
                f"But task have declared only {self.outputs_num} elements. "
                "If you need to split output for more elements, "
                "please use .split_output() or .set_outputs_num() function.")

    def __repr__(self) -> str:
        """
        Return a string representation of the Task instance.

        Returns:
            str: The name of the inner function.
        """
        return f"Task<{self.name}>"


class PipelineTask(Task):
    """
    A class that wraps Pipeline into Task.

    Attributes:
        pipeline (dagpipe.Pipeline): The instance of pipeline that .
    """

    def __init__(self, func, *args, name="auto", outputs_num=1, **kwargs):
        super().__init__(func, *args, name=name, outputs_num=outputs_num, **kwargs)
        self.pipeline: Pipeline = func.__self__

    def __repr__(self) -> str:
        return "Pipeline" + super().__repr__()


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
        args = ((instance,) + args)
        super().__init__(
            func, *args, name=name, outputs_num=outputs_num, **kwargs)

    def __repr__(self) -> str:
        """
        Return a string representation of the MethodTask instance in form
        TaskMethod<method name> where method name is:
        - 'ClassName.function_name' in most cases
        - 'ClassName' if __call__ method is wrapped.
        Returns:
            str: The name of the instance and the method.
        """
        return f"MethodTask<{self.name}>"

    def _get_function_name(self):
        if self.func.__name__ == "__call__":
            return self.instance.__class__.__name__
        return f"{self.instance.__class__.__name__}.{self.func.__name__}"


class TaskIterator:
    """Creates references to task
     that are pointing to specific elements from this task."""

    def __init__(self, task: Task) -> None:
        self._task = task
        self._ran_refs = []
        self._references = []
        self._index = 0
        self._outputs_num = self._task.outputs_num
        self.task_name = self._task.name

        for index in range(self._outputs_num):
            ref_name = self._task._get_ref_name(index)
            self._references.append(TaskReference(self, index, ref_name))

    def none_refs_ran(self) -> bool:
        """Checks if any of references ran."""
        return self._ran_refs == []

    def reset_ran_refs(self) -> None:
        """Forgets all tracked references."""
        self._ran_refs = []

    def register_ran_ref(self, new_ref: 'TaskReference') -> None:
        """Tracks new reference."""
        self._ran_refs.append(new_ref)

    def ref_already_ran(self, ref: 'TaskReference') -> bool:
        """Checks if given reference was ran already."""
        return ref in self._ran_refs

    def __iter__(self) -> 'TaskIterator':
        """Return self for iterating over."""
        return self

    def __next__(self):
        """
        Get the next output reference in the iteration.

        Returns:
            TaskReference: The next TaskReference object.

        Raises:
            StopIteration: If there are no more outputs to iterate over.
        """
        if self._index == self._outputs_num:
            raise StopIteration
        self._index += 1
        return self._references[self._index-1]

    @property
    def task(self) -> Task:
        """Return the task that is being iterated over."""
        return self._task


class TaskReference(Task):
    """
    A class representing a reference to a specific output of a Task.

    Attributes:
        task (Task): The original task this reference points to.
        ref_index (int): The index of the output this reference points to.
    """

    def __init__(self, task_iterator: TaskIterator, ref_index: int, name: str):
        """
        Initialize a TaskReference instance.

        Args:
            task_iterator (TaskIterator): Iterator from task this reference points to.
            ref_index (int): The index of the output this reference points to.
            name (str): The name of the reference.
        """
        task = task_iterator.task
        super().__init__(task.func, *task._params.args,
                         name=name, outputs_num=1, **task._params.kwargs)
        self._task: Task = task
        self.task_iterator: TaskIterator = task_iterator
        self.ref_index: int = ref_index

    def update_params(self, *args, **kwargs):
        """Update parameters for self and task that reference is pointing to."""
        self.linked_task.update_params(*args, **kwargs)
        self._params.update(*args, **kwargs)

    @property
    def linked_task(self):
        """Task that TaskReference is pointing to."""
        return self._task

    def run(self, *args, **kwargs) -> Any:
        """
        Execute the reference task if necessary and update the evaluated result.

        Args:
            *args: Positional arguments to override currently stored arguments.
            **kwargs: Keyword arguments to override currently stored arguments.

        Returns:
            Any: The result of the function execution.
        """
        if self._update_refs_and_check_if_all_ran():  # or (not self._task.evaluated_result):
            self._task.run(*args, **kwargs)
        self.evaluated_result = self._task.evaluated_result[self.ref_index]
        return self.evaluated_result

    def _update_refs_and_check_if_all_ran(self) -> bool:
        """
        Check if all references have run and update the reference tracking.

        Returns:
            bool: True if all references have run, False otherwise.
        """
        if self.task_iterator.none_refs_ran():
            self.task_iterator.register_ran_ref(self)
            return True
        if self.task_iterator.ref_already_ran(self):
            self.task_iterator.reset_ran_refs()
            self.task_iterator.register_ran_ref(self)
            return True
        else:
            self.task_iterator.register_ran_ref(self)
            return False

    def __repr__(self) -> str:
        """
        Return a string representation of the Task Reference instance.
        """
        return f"TaskRef{self.ref_index}<{self.name}>"


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
    def is_in(cls, collection: Iterable):
        """
        Check if any element in the given collection is an instance of StoppingTaskHolder.

        Args:
            collection (Iterable): The collection to be checked.

        Returns:
            bool: True if at least one element in the collection is a StoppingTaskHolder instance,
                  False otherwise.
        """
        return any(
            [isinstance(elem, StoppingTaskHolder) for elem in collection])

    def __repr__(self) -> str:
        return "STOPPED AT " + self.task.__repr__()
