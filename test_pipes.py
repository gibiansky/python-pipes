# Disable some Pylint warnings. Reasons:
#   - no-value-for-parameter: Generators confuse pylint. Retry after 1.6.
# pylint: disable=no-value-for-parameter
"""
Test cases for the pipes modules.
"""
import unittest

from hypothesis import given, example, assume # type: ignore
from hypothesis.strategies import text, lists, integers # type: ignore

from pipeline.pipes import (each, collect, cmd_source, cat, column, lines,
                            uniques, count, join)
from pipeline.utils import zcat, gzipped, temporary_file


@given(lists(text()))
@example([])
def each_collect(values):
    """
    Test that each() and collect() are inverses.
    """
    assert values == (each(*values) >> collect())


@given(text())
def gzipped_zcat(string):
    """
    Test that gzipped() and zcat() are inverses.
    """
    with temporary_file("gzipped_cat") as path:
        # Compress the file contents
        gzip_path = path + ".gz"
        each(string) >> gzipped(gzip_path)

        # Decompress the file contents
        contents = b"".join(chunk.content
                            for chunk in zcat(gzip_path) >> collect())

    assert string.encode("utf-8") == contents


@given(text())
def write_cat(string):
    """
    Test that writing to a file and reading with cat() are inverses.
    """
    with temporary_file("write_cat") as path:
        # Write the file contents
        each(string) >> path

        # Read the file contents
        contents = "".join(cat(path) >> collect())

    assert string.rstrip("\n") == contents.rstrip("\n")


@given(lists(text()))
def join_lines(strings):
    """
    Test that joining lines and then splitting on newlines does nothing.
    """
    results = each(*strings) >> lines() >> collect()
    assert "".join(results).rstrip("\n") == "".join(strings).rstrip("\n")


@given(lists(integers()))
def uniques_count(values):
    """
    Test that uniques() and counts() do not lose any values.
    """
    num_elements_uniques = sum((each(*values) >> uniques()).values())
    num_elements_count = each(*values) >> count()
    num_elements_true = len(values)

    assert num_elements_uniques == num_elements_true
    assert num_elements_count == num_elements_true


@given(lists(lists(text())))
def join_columns(test_lines):
    """
    Test that join() and columns() can be used to write data and read it back.
    """
    assume(all(all("\n" not in col and "\t" not in col and " " not in col
                   for col in line)
               for line in test_lines))
    assume([] not in test_lines)

    result_tab = each(*test_lines) >> join("\t") >> column(0) >> collect()
    result_space = (each(*test_lines) >>
                    join(" ") >>
                    column(0, sep=" ") >>
                    collect())

    first_columns = [line[0] for line in test_lines]
    assert result_tab == first_columns
    assert result_space == first_columns


class TestPipes(unittest.TestCase):
    """
    Unit tests for the pipes framework.
    """

    def test_cant_column_chunks(self):
        """
        Test that an exception is raised if you try to split the output of a
        command using columns. (Because the output is in chunks, not lines.)
        """
        with self.assertRaises(Exception):
            cmd_source("echo", "test") >> column(0) >> collect()

    def test_join_accepts_ints(self):
        """
        Test that we can pass integers to `join()`.
        """
        value = (each(["hello", 1, "bye"]) >> join(" ") >> collect())[0]
        self.assertEqual(value, "hello 1 bye")

    def test_join_accepts_floats(self):
        """
        Test that we can pass floating point numbers to `join()`.
        """
        value = (each(["hello", 1.3125, "bye"]) >> join(" ") >> collect())[0]
        self.assertEqual(value, "hello 1.3125 bye")
