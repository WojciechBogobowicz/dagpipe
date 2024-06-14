import os
import tempfile
import graphviz
from matplotlib import pyplot as plt

from lazy_pipe.task_core import Task
from lazy_pipe.pipeline import Pipeline


def visualize(pipeline: Pipeline, to_file: str | None = None):
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
    dot = graphviz.Digraph(format='png')
    for task in pipeline.tasks:
        dot.node(str(id(task)), repr(task))
        for arg in task.args + tuple(task.kwargs.values()):
            if isinstance(arg, Task):
                dot.edge(str(id(arg)), str(id(task)))
    return dot
