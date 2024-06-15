"""
lazy_pipe

A package for creating, managing, and visualizing pipelines of tasks.

Modules:
    task_core: Defines Task and MethodTask classes.
    decorators: Provides decorators for wrapping functions and methods 
        in Task and MethodTask instances.
    pipeline: Defines the Pipeline class for managing task execution.
    visualization: Provides functionality to visualize a pipeline of tasks 
        using graphviz.
"""

from lazy_pipe.task_decorators import task, method_task
from lazy_pipe.pipeline import Pipeline
from lazy_pipe.visualization import visualize
