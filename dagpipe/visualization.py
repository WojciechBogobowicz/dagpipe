"""
This module provides functionality to visualize a pipeline of tasks using graphviz.

Functions:
    visualize(pipeline, to_file=None): Display a pipeline of tasks 
                                       or save its visualization to a file.
"""

__all__ = ["visualize"]


import os
import tempfile
import graphviz
from matplotlib import pyplot as plt

from dagpipe.task_core import Task
from dagpipe.pipeline import Pipeline


def visualize(pipeline: Pipeline, to_file: str | None = None):
    """
    Visualize a pipeline of tasks using graphviz or save it to a file.
    Displayed image is generated by matplotlib.

    Args:
        pipeline (Pipeline): The pipeline to be visualized.
        to_file (str, optional): The file path to save the visualization. 
                                 If None, the visualization is displayed.
    """
    graph = _build_graph(pipeline)
    if to_file:
        graph.render(to_file, format='png', view=False)
    else:
        with tempfile.TemporaryDirectory() as tempdir:
            file_path = os.path.join(tempdir, 'graph')
            graph.render(file_path, format='png', view=False)
            img = plt.imread(file_path + ".png")
            plt.imshow(img)
            plt.axis("off")

def _build_graph(pipeline):
    """
    Execute the pipeline of tasks with optional initial arguments.

    Args:
        *args: Positional arguments to be passed to the initial task.
        **kwargs: Keyword arguments to be passed to the initial task.

    Returns:
        list: The evaluated results of the output tasks.
    """
    dot = graphviz.Digraph(format='png')
    for task in pipeline.tasks:
        # dot.node(str(id(task)), repr(task))
        dot.node(str(id(task)), task.name)
        for arg in task.args + tuple(task.kwargs.values()):
            if isinstance(arg, Task):
                dot.edge(str(id(arg)), str(id(task)))
    return dot

