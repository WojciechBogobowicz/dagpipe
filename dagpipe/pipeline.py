"""
This module defines the Pipeline class, 
which manages the execution of a sequence of tasks.

Classes:
    Pipeline: A class representing a pipeline of tasks to be executed in order.
"""

import re
from typing import Any, Callable, Iterable
from dagpipe.task_core import Task, TaskReference


class Pipeline:
    """
    A class defining order in which tasks should be executed.
    Allows to run them at once.

    Attributes:
        input (Task): The initial task of the pipeline.
        outputs (list[Task]): The list of output tasks of the pipeline.
        tasks (list[Task]): The list of tasks in the order they should be executed.
    """
    def __init__(self, 
                 inputs: Task | list[Task],
                 outputs: Task | list[Task],
                 conditional_stops: dict[str, Callable[[Any], bool]] = None):
        """
        Initialize a Pipeline instance.

        Args:
            input (Task): The initial task of the pipeline.
            outputs (list[Task]): The list of output tasks of the pipeline.
            conditional_stops (dict, Optional): key - task name
                function that would be evaluated on task output.
                If it returns true, then pipeline execution would be early stopped.
        """
        self.inputs: list[Task] = self._uniform_to_list(inputs, Task)
        self.outputs: list[Task] = self._uniform_to_list(outputs, Task)
        self.tasks: list[Task] = self._gather_tasks()
        self.conditional_stops = conditional_stops
        self._have_multi_input = (len(self.inputs) > 1)

    @staticmethod
    def _uniform_to_list(inputs, parsed_type: type):
        if isinstance(inputs, parsed_type):
            return [inputs]
        if isinstance(inputs, list):
            if all(isinstance(t, parsed_type) for t in inputs):
                return inputs
        raise ValueError(f"Only {parsed_type} type or lists of {parsed_type}s are acceptable. Got {inputs}")

    def to_task(self, *args, **kwargs) -> Task:
        """Create task that would execute self.run function with given *args, **kwargs"""
        pipeline_task = Task(self.run, *args, name=str(self), **kwargs)
        print("input", args, kwargs)
        print("stored in pipeline", pipeline_task.args, pipeline_task.kwargs)
        pipeline_task.update_args_if_provided(*args, **kwargs)
        print("stored in pipeline after update", pipeline_task.args, pipeline_task.kwargs)
        if len(self.outputs) > 1:
            names = [o.name for o in self.outputs]
            pipeline_task.split_output(names)
        return pipeline_task

    @classmethod
    def sequential(
            cls,
            tasks_sequence: Iterable,
            conditional_stops: dict[str, Callable[[Any], bool]] = None,
        ):
        """
        Create a Pipeline instance where tasks are executed sequentially.

        Args:
            tasks_sequence (Iterable): Tasks to be executed in sequence.

        Returns:
            Pipeline: A Pipeline instance with the tasks set to execute in sequence.
        """
        tasks_sequence = list(tasks_sequence)
        input_task = tasks_sequence.pop(0)
        input_ = input_task(Any)
        x = input_
        for task in tasks_sequence:
            x = task(x)
        return cls(input_, x, conditional_stops)

    def __getitem__(self, name: str) -> Task:
        repr_format_match = re.search(r"^.*Task.*<(.*)>", name)
        if repr_format_match:
            name = repr_format_match.groups()[0]
        tasks_from_references = [t.task for t in self.tasks if isinstance(t, TaskReference)]
        for task in self.tasks + tasks_from_references:
            if task.name == name:
                return task
        raise KeyError(f"Task with name {name} not found in self.tasks")

    def _gather_tasks(self) -> list[Task]:
        """
        Gather all tasks in the pipeline in the order they should be executed.

        Returns:
            list[Task]: The list of tasks in execution order.
        """
        children_num = self.__count_tasks_children()
        tasks = self.__order_tasks(children_num)
        self.__assert_all_inputs_seen(tasks)
        return tasks

    def __assert_all_inputs_seen(self, tasks):
        for input_ in self.inputs:
            if not input_ in tasks:
                raise RuntimeError(
                    "Unable to build graph. "
                   f"No connection between {input_} and {self.outputs} found. "
                    "Verify if pipeline is builded correctly."
                )
        
    def __count_tasks_children(self):
        children_num = dict()
        seen = set()
        stack = list(self.outputs)
        while stack:
            current_task = stack.pop()
            if current_task not in seen:
                seen.add(current_task)
                for parent in current_task.args + tuple(current_task.kwargs.values()):
                    if isinstance(parent, Task):
                        stack.append(parent)
                        children_num[id(parent)] = children_num.get(id(parent), 0) + 1
        return children_num
    
    def __order_tasks(self, children_num):
        tasks = []
        seen = set(self.outputs)
        stack = list(self.outputs)
        while stack:
            current_task = stack.pop()
            tasks.append(current_task)
            seen.add(current_task)
            for parent in current_task.args + tuple(current_task.kwargs.values()):
                if isinstance(parent, Task):
                    children_num[id(parent)] = children_num.get(id(parent), 0) - 1
                    if children_num[id(parent)] == 0: #and (parent not in seen):
                        stack.append(parent)
                        seen.add(parent)
        tasks.reverse()
        return tasks


    def run(self, *single_input_args, **single_or_multi_input_kwargs) -> list[Task]:
        """
        Execute the pipeline of tasks with optional initial arguments.

        Args:
            *single_input_args: 
                Positional arguments to be passed to the input tasks.
                This param is ignored in when pipeline have multiple inputs.
            **single_or_multi_input_kwargs: 
                Keyword arguments to be passed to the initial task.
            The currently stored keyword arguments would be updated.
            When the pipeline has multiple inputs, these keyword arguments 
            should be in the format {task_name: args_or_kwargs}, where 
            args_or_kwargs can be represented as single value or tuple 
            what would be threated as args, or dictionary that would be 
            threated as kwargs, eventually tuple[tuple, dict] that would 
            be translated to (args, kwargs).

        Returns:
            list: The evaluated results of the output tasks.
        """
        self._setup_input(single_input_args, single_or_multi_input_kwargs)
        for task in self.tasks:
            task.run()
            if self.conditional_stops:
                if task.name in self.conditional_stops:
                    if self.conditional_stops[task.name](task.evaluated_result):
                        return [task.evaluated_result, (task.to_stopping_holder())]
        return [output.evaluated_result for output in self.outputs]

    def _setup_input(self, single_input_args, single_or_multi_input_kwargs):
        if self._have_multi_input:
            self._update_args_for_multi_input(single_or_multi_input_kwargs)
        else:
            self.inputs[0].update_args_if_provided(
                *single_input_args, **single_or_multi_input_kwargs)

    def _update_args_for_multi_input(self, single_or_multi_input_kwargs: dict):
        for task_name, arg_or_kwarg in single_or_multi_input_kwargs.items():
            task = self[task_name]
            if not task in self.inputs:
                raise ValueError(f"{task} is not in inputs.")
            args, kwargs = self._parse_args_or_kwargs(arg_or_kwarg)
            task.update_args_if_provided(*args, **kwargs)

    @staticmethod
    def _parse_args_or_kwargs(arg_or_kwarg: Any | tuple | dict):
        if isinstance(arg_or_kwarg, tuple):
            if (
                (len(arg_or_kwarg) == 2) 
                and isinstance(arg_or_kwarg[0], tuple)
                and isinstance(arg_or_kwarg[1], dict)
            ):
                args_, kwargs_ = arg_or_kwarg
            else:
                args_, kwargs_ = arg_or_kwarg, {}
        elif isinstance(arg_or_kwarg, dict):
            args_, kwargs_ = tuple(), arg_or_kwarg
        else:
            args_, kwargs_ = (arg_or_kwarg, ), {}
        return args_, kwargs_
    
    def with_outputs(self, outputs_names: str | list[str]):
        """Create new pipeline with inputs and conditional stops from self.


        Args:
            outputs_names (Task | list[Task]): _description_

        Returns:
            dagpipe.Pipeline: New pipeline, with changed outputs
        """
        outputs_names = self._uniform_to_list(outputs_names, str)
        outputs = [self[name] for name in outputs_names]
        
        return Pipeline(self.inputs, outputs, self.conditional_stops)

    def __repr__(self) -> str:
        return f"Pipeline(in: {self.inputs}, out: {self.outputs})"
