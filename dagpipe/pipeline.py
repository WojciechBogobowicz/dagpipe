"""
This module defines the Pipeline class, 
which manages the execution of a sequence of tasks.

Classes:
    Pipeline: A class representing a pipeline of tasks to be executed in order.
"""


from dagpipe.task_core import Task


class Pipeline:
    """
    A class defining order in which tasks should be executed.
    Allows to run them at once.

    Attributes:
        input (Task): The initial task of the pipeline.
        outputs (list[Task]): The list of output tasks of the pipeline.
        tasks (list[Task]): The list of tasks in the order they should be executed.
    """
    def __init__(self, input: Task, outputs: list[Task]):
        """
        Initialize a Pipeline instance.

        Args:
            input (Task): The initial task of the pipeline.
            outputs (list[Task]): The list of output tasks of the pipeline.
        """
        self.input: Task = input
        self.outputs: list[Task] = outputs
        self.tasks: list[Task] = self._gather_tasks()

    def _gather_tasks(self) -> list[Task]:
        """
        Gather all tasks in the pipeline in the order they should be executed.

        Returns:
            list[Task]: The list of tasks in execution order.
        """
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
                        children_num[repr(parent)] = children_num.get(repr(parent), 0) + 1

        tasks = []
        seen = set(self.outputs)
        stack = list(self.outputs)
        while stack:
            current_task = stack.pop()
            tasks.append(current_task)
            seen.add(current_task)
            for parent in current_task.args + tuple(current_task.kwargs.values()):
                if isinstance(parent, Task):
                    children_num[repr(parent)] = children_num.get(repr(parent), 0) - 1
                    if (children_num[repr(parent)] == 0): #and (parent not in seen):
                        stack.append(parent)
                        seen.add(parent)

        tasks.reverse()
        return tasks

    def run(self, *args, **kwargs) -> list[Task]:
        """
        Execute the pipeline of tasks with optional initial arguments.

        Args:
            *args: Positional arguments to be passed to the initial task.
                Currently stored args would be replaced in total.
            **kwargs: Keyword arguments to be passed to the initial task.
                Currently stored kwargs would be updated.

        Returns:
            list: The evaluated results of the output tasks.
        """
        self.input.update_args_if_provided(*args, **kwargs)
        for task in self.tasks:
            task.run()
        return [output.evaluated_result for output in self.outputs]
