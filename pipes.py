# Ignore some pylint warnings. Reasons:
#  - missing-docstring: Decorators make this hard, since you generate functions
#  - no-value-for-parameter: Pylint makes mistakes on decorated functions.
#                            Try removing this after 1.6 is released.
# pylint: disable=missing-docstring,no-value-for-parameter
"""
This module defines a library for streaming operations, composed using the `>>`
operator. This allows for easy to read and write shell-like pipelines, such as:

    # Count the number of lines on standard input.
    num_lines = cat("/dev/stdin") >> count()

    # Count the number of unique values in the 3rd column.
    # Return a counter of values, e.g. {"a": 10, "b": 3}.
    counts = cat("/dev/stdin") >> column(3) >> uniques()

### Pipeline Components: Sources, Pipes, and Sinks

This library relies on a fundamental streaming abstraction consisting of three
parts: sources, pipes, and sinks. You can view a pipeline as a source, followed
by zero or more pipes, followed by a sink. Visually, this looks like:

   source >> pipe1 >> pipe2 >> ... >> pipeN >> sink

A source creates values and streams them "downstream"; in the diagram above,
this is to the right of the source. A pipe consumes values from "upstream" and
then yields values "downstream". A pipe can consume any number of values and
yield any number of values; it can yield more or less values than it consumes.
It is not obligated to consume all values the source produces. Finally, a sink
consumes values from "upstream" and uses them. A sink may return a value; if it
does, the value of the pipeline is the returned value.

Until a pipeline is connected to a sink, nothing is done and no code is
executed.

(Sources, pipes, and sinks are represented internally by the the Source, Pipe,
and Sink classes.  You should never need to subclass these classes. Use the
provided decorators to create instances instead.)

### Creating Custom Components

Creating custom sources, pipes, and sinks is supported through a variety of
helper decorators.

For creating sources, use the `source` decorator.
For creating sinks, use the `sink` decorator.
For creating pipes, use the `transform`, `foreach`, and `pipe` decorators.

### Built-in Components

This library defines a variety of custom pipeline components.

  - each:           Stream provided values.
  - cat:            Stream lines of a file.
  - cmd_source:     Stream chunks from the stdout of a command.

  - cmd_pipe:       Use a command as a pipe, like a Unix pipe.
  - lines:          Convert text chunks to lines.

  - count:          Count the number of values in the stream.
  - uniques:        Count unique items in the stream in a counter.
  - write:          Stream lines to a file.
  - cmd_sink:       Stream chunks to the stdin of a command.

In addition to the above components, raw strings may be used as sinks, in which
case they are interpreted as filenames to which data should be written.
"""

from typing import Iterator, Tuple, TypeVar, Union, Callable, Any  # NOQA
from collections import Counter, namedtuple, defaultdict
from numbers import Number
from threading import Thread
import os
import subprocess


PipeSinkStr = TypeVar('PipeSinkStr', str, 'Pipe', 'Sink')
PipeFuncOutput = TypeVar('PipeFuncOutput')
PipeFuncInput = TypeVar('PipeFuncInput')


class Pipe(object):
    """
    A pipe is an object that consumes values from upstream and yields values
    downstream. A pipe can yield more or less values than it consumes.

    A pipe sits between to `>>`s in a chain of `>>` operators.
    """
    def __init__(self, func: Callable[[Iterator[PipeFuncInput]],
                                      Iterator[PipeFuncOutput]]
                 ) -> None:
        self.func = func

    def run(self, generator: Iterator[PipeFuncInput]
            ) -> Iterator[PipeFuncOutput]:
        """Return a generator of values to yield downstream."""
        for x in self.func(generator):
            yield x

    def __rshift__(self, rhs: PipeSinkStr) -> Union['Pipe', 'Sink']:
        """
        The `>>` operator for the pipes library.

        Usually this is not used, and instead the implementation on `Source`
        is used. This will only be used if you do not start a pipe chain with a
        `Source`, but rather with a `Pipe`.

        A `Pipe` may be combined with another `Pipe` to form a `Pipe`.
        A `Pipe` may be combined with a `Sink` to form a `Sink`.
        (Strings are treated as file sinks!)
        """
        if isinstance(rhs, Pipe):
            return Pipe(lambda generator: rhs.run(self.run(generator)))
        elif isinstance(rhs, Sink):
            return Sink(lambda generator: rhs.run(self.run(generator)))
        elif isinstance(rhs, str):
            return Sink(lambda generator: write(rhs).run(self.run(generator)))
        else:
            raise TypeError("Unexpected pipe type {0}".format(type(rhs)))


class Sink(object):
    """
    A sink is an object which consumes objects from upstream and creates a
    value out of them, potentially having a side-effect in the process.

    A stream ends a chain of `>>` operators.
    """
    def __init__(self, func):
        self.func = func

    def run(self, generator):
        """Run the sink by passing the source generator to it."""
        return self.func(generator)


class Source(object):
    """
    A source is an object that yield values downstream.
    A source is like a Python generator which supports the `>>` operator.

    A stream begins a chain of `>>` operators.
    """
    def __init__(self, generator):
        self.generator = generator

    def __rshift__(self, rhs):
        """
        The `>>` operator for the pipes library.

        This operator works by examining the type of the value it is passed.
        If it is a pipe, it collapses this source with it into a new source.
        If it is a sink, it joins the source and sink and yields a value.
        If it is a string, it treats it as a sink for a file.
        """
        if isinstance(rhs, Pipe):
            return Source(value for value in rhs.run(self.generator))
        elif isinstance(rhs, Sink):
            return rhs.run(self.generator)
        elif isinstance(rhs, str):
            return write(rhs).run(self.generator)
        else:
            raise TypeError("Unknown pipe type {0}".format(type(rhs)))


def source(generator):
    """
    The `source` decorator turns a function into a source.

    The function must yield values downstream. For example:

      @source
      def cat(filename):
          with open(filename, "r") as handle:
              for line in handle:
                  yield line
    """
    # Pass all arguments to the generator
    def f(*args, **kwargs):
        return Source(generator(*args, **kwargs))
    f.__doc__ = generator.__doc__
    return f


def sink(consumer):
    """
    The `sink` decorator turns a function into a sink. The function receives as
    its first argument a generator to read from. For example:

      @sink
      def write(generator, filename):
          with open(filename, "w") as handle:
              for line in generator:
                  handle.write(line)
    """
    # Pass all arguments to the generator
    def f(*args, **kwargs):
        return Sink(lambda g: consumer(g, *args, **kwargs))
    f.__doc__ = consumer.__doc__
    return f


def transform(transformation):
    """
    The `transform` decorator turns a function into a pipe. It is the
    equivalent of `map` for pipes. The function receives one value for upstream
    and the return value of the function is yielded downstream. For example:

      @transform
      def strip(line):
        return line.strip()
    """
    def f(*args, **kwargs):
        def t(generator):
            for value in generator:
                yield transformation(value, *args, **kwargs)
        return Pipe(t)
    f.__doc__ = transformation.__doc__
    return f


def pipe(p):
    """
    The `pipe` decorator turns a function into a pipe. It is the
    most general way to create a pipe, and can be used for pipes that need to
    store local state. The function receives a generator representing the
    upstream values and can yield values downstream.
    """
    def f(*args, **kwargs):
        return Pipe(lambda generator: p(generator, *args, **kwargs))
    f.__doc__ = p.__doc__
    return f


def foreach(transformation):
    """
    The `foreach` decorator turns a function into a pipe. It acts similar to
    `transform` but allows the function to yield any number of values
    downstream (including no values). The function receives a value from
    upstream as an argument, and may yield values to send them downstream.

    For example, we can implement an equivalent of `filter`:

      @foreach
      def require(value, predicate):
          if predicate(value):
              yield value
    """
    def f(*args, **kwargs):
        def t(generator):
            for value in generator:
                for yielded in transformation(value, *args, **kwargs):
                    yield yielded
        return Pipe(t)
    f.__doc__ = transformation.__doc__
    return f


@source
def read(*filenames):
    """Read chunks from a file."""
    chunk_size = 1024
    for filename in filenames:
        with open(filename, "rb") as handle:
            while True:
                data = handle.read(chunk_size)

                # Yield the data, unless this is EOF, in which case, exit the
                # loop and move on to the next file.
                if data:
                    yield Chunk(data)
                else:
                    break


def cat(*filenames):
    """Read lines from files, one by one."""
    return read(*filenames) >> lines()


@sink
def write(generator, filename):
    """Write lines to a file."""
    with open(filename, "wb") as handle:
        for line in generator:

            if isinstance(line, Chunk):
                line = line.content

            if isinstance(line, str):
                line = line.encode('utf-8')

            handle.write(line)


@foreach
def require(value, predicate):
    """Filter a stream. Pass downstream values that match the predicate."""
    if predicate(value):
        yield value


@pipe
def filter_by_index(generator, indices):
    """Select elements from a stream by their indices."""
    for index, value in enumerate(generator):
        if index in indices:
            yield index, value


@sink
def count(generator):
    """Count the number of items in the stream."""
    counter = 0
    for _ in generator:
        counter += 1
    return counter


def feed_stdin(proc, chunks):
    """Feed chunks from upstream as stdin to a process."""
    try:
        for chunk in chunks:
            # Unwrap Chunks into text
            if isinstance(chunk, Chunk):
                chunk = chunk.content

            if isinstance(chunk, str):
                chunk = chunk.encode('utf-8')
            proc.stdin.write(chunk)
    finally:
        # Make sure to close the handle even in case of exceptions, otherwise
        # an exception upstream will cause this to loop indefinitely.
        proc.stdin.close()

# In order to make sure that people don't treat cmd_source, cmd_pipe, etc, as
# operating line-by-line, wrap the content in them in Chunks. Then, the lines()
# pipe will be able to convert Chunks into text.
Chunk = namedtuple("Chunk", "content")


@pipe
def cmd_pipe(stdin_chunks, *cmd: str, **kwargs):
    """
    Use a shell command as a pipe.

    Outputs chunks, not lines.
    """
    environment = kwargs.get("env", {}).copy()
    environment.update(os.environ)

    # bufsize=-1 uses system default (fully buffered)
    proc = subprocess.Popen(cmd, bufsize=-1, env=environment,
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE)

    # Start a separate thread to feed input to the process
    stdin_thread = Thread(target=feed_stdin, args=(proc, stdin_chunks))
    stdin_thread.start()

    # Read from the process until it is done
    while True:
        bufsize = 8192
        chunk = proc.stdout.read(bufsize)

        # If chunk is empty, means we've reached EOF
        if not chunk:
            break

        yield Chunk(chunk)

    exit_code = proc.wait()
    stdin_thread.join()

    if exit_code != 0:
        raise ValueError("Command '{0}' returned non-zero exit status: {1}"
                         .format(" ".join(cmd), exit_code))


def cmd_source(*cmd: str):
    """
    Use a shell command as a source.

    Outputs chunks, not lines.
    """
    return each() >> cmd_pipe(*cmd)


@sink
def cmd_sink(stdin_chunks, *cmd, **kwargs):
    """Use a shell command as a sink."""
    environment = kwargs.get("env", {}).copy()
    environment.update(os.environ)
    # bufsize=-1 uses system default (fully buffered)
    proc = subprocess.Popen(cmd, bufsize=-1, env=environment,
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE)

    # Feed input to this process, then wait for it.
    feed_stdin(proc, stdin_chunks)
    proc.wait()
    return proc.stdout.read()


@pipe
def lines(generator: Iterator[Union[Chunk, str, bytes]]) -> Iterator[str]:
    """
    Convert a stream of text chunks into a stream of lines.

    Lines are terminated by the newline character '\\n'.

    The last yielded line will always end in a newline, even if the input
    stream did not have a newline.
    """
    remainder = b""
    for chunk in generator:
        # Allow Chunks as input in addition to text
        if isinstance(chunk, Chunk):
            unchunk = chunk.content
        else:
            unchunk = chunk

        if isinstance(unchunk, str):
            bchunk = unchunk.encode("utf-8")
        else:
            bchunk = unchunk

        chunk_lines = bchunk.split(b"\n")
        if chunk_lines:
            # Absorb remainder into next chunk
            chunk_lines[0] = remainder + chunk_lines[0]
            remainder = chunk_lines[-1]

            for line in chunk_lines[:-1]:
                yield line.decode("utf-8") + "\n"
    if remainder:
        yield remainder.decode("utf-8") + "\n"


@source
def each(*args):
    """Yield all values passed in as arguments."""
    for arg in args:
        yield arg


@sink
def uniques(generator):
    """
    Count the number of unique items in a stream.

    Return a dictionary mapping items to counts."""
    counter = Counter()
    for value in generator:
        counter[value] += 1
    return dict(counter)


@transform
def column(value, *columns, **kwargs):
    """
    Extract one or more columns from a tab-separated format. If more than one
    index is provided, a list is returned; if only one index is provided, only
    the value is returned.

    The value of `sep` is used to split the columns, so for example, to parse
    CSV files, you could use something like column(1, 2, sep=","). By default
    tabs are used.
    """
    pieces = value.split(kwargs.get("sep", "\t"))
    if len(columns) == 1:
        return pieces[columns[0]]
    else:
        return [pieces[i] for i in columns]


@transform
def join(values, sep):
    """
    Join together values with a string separator.

    This is shorthand for sep.join(...).
    """
    return sep.join(str(value) if isinstance(value, Number) else value
                    for value in values)


@transform
def unlines(string):
    """Add a line to the end of every string."""
    return string + "\n"


def tsv():
    """Convert a stream of lists into a tsv, yielding all the lines."""
    return join("\t") >> unlines()


@sink
def collect(generator):
    """Collect all items in the stream into a list."""
    return list(generator)


@sink
def print_stream(generator):
    """Print items in the stream."""
    for val in generator:
        print(val)


@sink
def count_groups(generator):
    """
    Count the number of distinct elements in the stream, assuming that the
    elements come in groups.

    For example:
      >>> each(1, 2, 1, 10, 10, 5, 5) >> count_groups()
      5
    """
    # Instead of using a sentinel value, just look at the first value output by
    # the generator, and catch the exception if it is empty.
    try:
        last_element = next(generator)
        num_groups = 1
    except StopIteration:
        return 0

    for element in generator:
        if element != last_element:
            num_groups += 1
            last_element = element
    return num_groups


@transform
def map_stream(value, function):
    """Applies function to stream."""
    return function(value)


@pipe
def group_by(generator, function):
    """Group elements on key produced by applying function."""
    # Initialize the first group and group key (the output of function)
    # We cannot just use a sentinel value, since any value we use could
    # be output by the function
    try:
        first_element = next(generator)
        current_key = function(first_element)
        current_group = [first_element]
    except StopIteration:
        return

    for element in generator:
        key = function(element)
        if key == current_key:
            current_group.append(element)
        else:
            yield current_key, current_group
            current_key = key
            current_group = [element]

    # Yield any straglers (if there are any)
    if len(current_group) > 0:
        yield current_key, current_group


@sink
def collect_groups(generator, function):
    """Returns a dict with all generator elements grouped by key."""
    grouped_elements = defaultdict(list)
    for element in generator:
        grouped_elements[function(element)].append(element)
    # covert to normal dict before returning
    return dict(grouped_elements)
