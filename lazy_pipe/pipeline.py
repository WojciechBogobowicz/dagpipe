from lazy_pipe.task_core import Task


class Pipeline:
    def __init__(self, input: Task, outputs: list[Task]):
        self.input: Task = input
        self.outputs: list[Task] = outputs
        self.tasks: list[Task] = self._gather_tasks()


    def _gather_tasks(self) -> list[Task]:
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
        self.input.update_args_if_provided(*args, **kwargs)
        for task in self.tasks:
            task.run()
        return [output.evaluated_result for output in self.outputs]
