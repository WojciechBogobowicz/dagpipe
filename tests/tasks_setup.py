"""Setup for all tasks used in testing"""
import dagpipe


class SelfExecutionsCounter:
    """
    Execute function and tracks how many times it was executed inside
    <func_name>_counter variable.
    """

    def __init__(self) -> None:
        self.do_nothing_counter = 0

    @dagpipe.method_task()
    def do_nothing(self, inp):
        """Pass input further and counts its execution."""
        self.do_nothing_counter += 1
        return inp


@dagpipe.task
def do_nothing(x):
    """does nothing"""
    return x


@dagpipe.task
def add_1(x):
    """adds 1 to the input"""
    return x + 1


@dagpipe.task()
def zip_two_inputs(x, y):
    """zips two inputs together"""
    return x, y


@dagpipe.task()
def zip_inputs(*args):
    """zips inputs together"""
    return args


@dagpipe.task()
def split_to_two(x):
    """splits into two"""
    return x[0], x[1]


@dagpipe.task()
def append_a(x):
    """appends a to the input"""
    return [x, 'a']


@dagpipe.task()
def append_b(x):
    """appends b to the input"""
    return [x, 'b']


@dagpipe.task()
def append_c(x):
    """appends c to the input"""
    return [x, 'c']


@dagpipe.task()
def append_d(x):
    """appends d to the input"""
    return [x, 'd']


@dagpipe.task()
def append_e(x):
    """appends e to the input"""
    return [x, 'e']
