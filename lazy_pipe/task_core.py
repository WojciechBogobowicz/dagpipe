from typing import Any


class Task:
    def __init__(self, func, *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.evaluated_result = None

    def run(self, *args, **kwargs) -> Any:
        self.update_args_if_provided(*args, **kwargs)
        args, kwargs = self.unpack_args_from_results()
        self.evaluated_result = self.evaluate_result(*args, **kwargs)
        return self.evaluated_result

    def evaluate_result(self, *args, **kwargs) -> Any:
        return self.func(*args, **kwargs)

    def update_args_if_provided(self, *args, **kwargs) -> None:
        if args or kwargs:
            self.args = args if args else self.args
            self.kwargs.update(kwargs)

    def unpack_args_from_results(self) -> tuple[tuple, dict]:
        args = tuple(a if not isinstance(a, Task) else a.evaluated_result for a in self.args)
        kwargs = {k: v if not isinstance(v, Task) else v.evaluated_result for k, v in self.kwargs.items()}
        return args, kwargs

    def __repr__(self) -> str:
        return self.func.__name__

class MethodTask(Task):
    def __init__(self, instance, func, *args, **kwargs):
        super().__init__(func, *args, **kwargs)
        self.instance = instance

    def evaluate_result(self, *args, **kwargs) -> Any:
        return self.func(self.instance, *args, **kwargs)

    def __repr__(self) -> str:
        if self.func.__name__ == "__call__":
            return self.instance.__class__.__name__
        return f"{self.instance.__class__.__name__}.{self.func.__name__}"
