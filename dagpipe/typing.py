from typing import Union

from dagpipe.task_core import MethodTask, PipelineTask, Task, TaskReference


TaskType = Union[Task, TaskReference, MethodTask, PipelineTask]