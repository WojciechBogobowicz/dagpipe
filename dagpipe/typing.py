from typing import Any, Callable, Union

from dagpipe.task_core import MethodTask, PipelineTask, Task, TaskReference


TaskType = Union[Task, TaskReference, MethodTask, PipelineTask]

TaskDecoratorType = (Callable[[Callable], Callable[[Any], Task]]
                     | Callable[[Any], Task])

MethodTaskDecoratorType = (Callable[[Callable], Callable[[Any], MethodTask]]
                           | Callable[[Any], MethodTask])
