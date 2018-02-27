# Pipes

A streaming framework in Python, roughly inspired by the Haskell
[pipes](https://hackage.haskell.org/package/pipes) library.

The module is in `pipes.py`.


This module defines a library for streaming operations, composed using the `>>`
operator. This allows for easy to read and write shell-like pipelines, such as:

    # Count the number of lines on standard input.
    num_lines = cat("/dev/stdin") >> count()
    # Count the number of unique values in the 3rd column.
    # Return a counter of values, e.g. {"a": 10, "b": 3}.
    counts = cat("/dev/stdin") >> column(3) >> uniques()
    
## Pipeline Components: Sources, Pipes, and Sinks

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

## Creating Custom Components

Creating custom sources, pipes, and sinks is supported through a variety of
helper decorators.
For creating sources, use the `source` decorator.
For creating sinks, use the `sink` decorator.
For creating pipes, use the `transform`, `foreach`, and `pipe` decorators.

## Built-in Components

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
