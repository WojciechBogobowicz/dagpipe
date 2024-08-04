"""Setup for all tasks used in testing"""
import dagpipe


DEBUG = True


def debug_print(func):
    def wrapper(*args, **kwargs):
        if DEBUG:
            print(f"Function: {func.__name__}")
            print(f"Inputs: args={args}, kwargs={kwargs}")

        result = func(*args, **kwargs)

        if DEBUG:
            print(f"Output: {result}")

        return result

    return wrapper


class SelfExecutionsCounter:
    """
    Execute function and tracks how many times it was executed inside
    <func_name>_counter variable.
    """

    def __init__(self) -> None:
        self.do_nothing_counter = 0

    @dagpipe.method_task()
    @debug_print
    def do_nothing(self, inp):
        """Pass input further and counts its execution."""
        self.do_nothing_counter += 1
        return inp


@dagpipe.task
@debug_print
def do_nothing(x):
    """does nothing"""
    return x


@dagpipe.task
@debug_print
def add_1(x):
    """adds 1 to the input"""
    return x + 1


@dagpipe.task()
@debug_print
def zip_two_inputs(x, y):
    """zips two inputs together"""
    return x, y


@dagpipe.task()
@debug_print
def zip_inputs(*args):
    """zips inputs together"""
    return args


@dagpipe.task()
@debug_print
def split_to_two(x):
    """splits into two"""
    return x[0], x[1]


@dagpipe.task()
@debug_print
def append_a(x):
    """appends a to the input"""
    return [x, 'a']


@dagpipe.task()
@debug_print
def append_b(x):
    """appends b to the input"""
    return [x, 'b']


@dagpipe.task()
@debug_print
def append_c(x):
    """appends c to the input"""
    return [x, 'c']


@dagpipe.task()
@debug_print
def append_d(x):
    """appends d to the input"""
    return [x, 'd']


@dagpipe.task()
@debug_print
def append_e(x):
    """appends e to the input"""
    return [x, 'e']
