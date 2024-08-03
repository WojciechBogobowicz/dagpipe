import unittest


import dagpipe
from tests.tasks_setup import (
    SelfExecutionsCounter,
    add_1,
    append_a,
    append_b,
    append_c,
    do_nothing,
    split_to_two,
    zip_inputs,
    zip_two_inputs
)


class PipelineRunTest(unittest.TestCase):
    """testing run function in different configurations"""

    def setUp(self) -> None:
        self.simple_pipeline_sequential = dagpipe.Pipeline.sequential(
            [add_1, add_1, add_1])

        # simple pipeline with normal initialization
        inp = add_1("Input that should be overwritten")
        x = add_1(inp)
        x = add_1(x)
        self.simple_pipeline = dagpipe.Pipeline(inp, [x])

        # pipeline that splits in middle
        inp = do_nothing("Input that should be overwritten")
        x_a = append_a(inp)
        x, a = split_to_two(x_a).split_output(["x", "a"])
        x_b = append_b(x)
        a_c = append_c(a)
        self.middle_split_pipeline = dagpipe.Pipeline([inp], [x_b, a_c])

        # pipeline that splits at the end
        inp = do_nothing("Input that should be overwritten")
        x_a = append_a(inp)
        x, a = split_to_two(x_a).split_output(["x", "a"])
        self.end_split_pipeline = dagpipe.Pipeline([inp], [x, a])

        # pipeline that splits asymmetrically
        inp = do_nothing("Input that should be overwritten")
        x_a = append_a(inp)
        x, a = split_to_two(x_a).split_output(["x", "a"])
        a = do_nothing(a)
        self.asymmetric_split_pipeline = dagpipe.Pipeline([inp], [x, a])

        # pipeline with two inputs
        inp1 = do_nothing("Input that should be overwritten").set_name("inp1")
        inp2 = do_nothing("Input that should be overwritten").set_name("inp2")
        x = zip_two_inputs(inp1, inp2)
        self.two_inputs_pipeline = dagpipe.Pipeline([inp1, inp2], [x])

        # check references execution count pipeline
        self.executions_counter = SelfExecutionsCounter()
        inp = do_nothing("Input that should be overwritten")
        x1, x2, x3, out2 = (
            self.executions_counter
            .do_nothing(inp)
            .split_output(["x1", "x2", "x3", "x4"])
        )
        out1 = zip_inputs(x1, x2, x3)
        self.pipeline_with_execution_counter = (
            dagpipe.Pipeline(inp, [out1, out2])
        )

    def test_simple_pipeline(self):
        """run simple_pipeline twice and checks output"""
        out1 = self.simple_pipeline.run(0)
        out2 = self.simple_pipeline.run(10)

        self.assertEqual(out1, [3])
        self.assertEqual(out2, [13])

    def test_simple_sequential_pipeline(self):
        """run simple_pipeline_sequential twice and checks output"""
        out1 = self.simple_pipeline_sequential.run(0)
        out2 = self.simple_pipeline_sequential.run(10)

        self.assertEqual(out1, [3])
        self.assertEqual(out2, [13])

    def test_middle_split_pipeline(self):
        """run middle_split_pipeline twice and checks output"""
        out1 = self.middle_split_pipeline.run("x")
        out2 = self.middle_split_pipeline.run("y")

        self.assertEqual(out1, [["x", "b"], ["a", "c"]])
        self.assertEqual(out2, [["y", "b"], ["a", "c"]])

    def test_end_split_pipeline(self):
        """run end_split_pipeline twice and checks output"""
        out1 = self.end_split_pipeline.run("x")
        out2 = self.end_split_pipeline.run("y")

        self.assertEqual(out1, ["x", "a"])
        self.assertEqual(out2, ["y", "a"])

    def test_asymmetric_split_pipeline(self):
        """run asymmetric_split_pipeline twice and checks output"""
        out1 = self.asymmetric_split_pipeline.run("x")
        out2 = self.asymmetric_split_pipeline.run("y")

        self.assertEqual(out1, ["x", "a"])
        self.assertEqual(out2, ["y", "a"])

    def test_two_inputs_pipeline(self):
        """run two inputs pipeline twice and checks output"""
        out1 = self.two_inputs_pipeline.run(inp1="x", inp2="y")
        out2 = self.two_inputs_pipeline.run(inp1="x2", inp2="y2")

        self.assertEqual(out1, [("x", "y")])
        self.assertEqual(out2, [("x2", "y2")])

    def test_pipeline_with_execution_counter(self):
        """run two inputs pipeline twice and checks output"""
        out1 = self.pipeline_with_execution_counter.run((1, 2, 3, 4))
        self.assertEqual(
            self.executions_counter.do_nothing_counter, 1,
            "TaskReferences ran task that they point to, "
            "too many/not enough times.")
        self.assertEqual(out1, [(1, 2, 3), 4])

        out2 = self.pipeline_with_execution_counter.run((10, 20, 30, 40))
        self.assertEqual(
            self.executions_counter.do_nothing_counter, 2,
            "TaskReferences ran task that they point to, "
            "too many/not enough times.")
        self.assertEqual(out2, [(10, 20, 30), 40])
