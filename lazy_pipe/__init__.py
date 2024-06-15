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

try:
    from lazy_pipe.visualization import visualize
except ImportError:
    def visualize(pipeline: Pipeline, to_file: str | None = None):
        """Alternative visualize import when visualization tools aren't installed.

        Args:
            pipeline (Pipeline): Dummy parameter for compatibility.
            to_file (str | None, optional): Dummy parameter for compatibility.

        Raises:
            ImportError: Throws it always when executed.
        """
        raise ImportError(
            "There is no graphviz or matplotlib library in your "
            "environment, so visualize method could not be used. "
            "If you want to use this package install this package with "
            "pip install lazy_pipe[viz].")

