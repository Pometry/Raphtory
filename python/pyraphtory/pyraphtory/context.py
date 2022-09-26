import inspect
import os
import re
import subprocess
import json

import pandas as pd
from abc import abstractmethod
from pathlib import Path
from subprocess import PIPE, Popen
from threading import Thread
from weakref import finalize
import pyraphtory.fileutils as fileutils

from py4j.java_gateway import JavaGateway, GatewayParameters
from py4j.protocol import Py4JJavaError
from typing import IO, AnyStr

import pyraphtory.algorithm
import pyraphtory.interop
import pyraphtory.scala.collection
import pyraphtory.vertex
import pyraphtory.graph
from pyraphtory import interop, __version__
import sys


def _kill_jvm(j_raphtory, j_gateway):
    interop.logger.info("Shutting down pyraphtory")
    try:
        j_raphtory.kill()
    finally:
        pass
    try:
        j_gateway.close()
    finally:
        pass


def print_output(input_stream: IO[AnyStr], output_stream=sys.stdout):
    out = input_stream.readline()
    while out:
        output_stream.write(out.decode("utf-8"))
        out = input_stream.readline()


def read_output(stream: IO[AnyStr], logging=False):
    out = stream.readline()
    while out:
        yield out
        out = stream.readline()
        if logging:
            sys.stdout.write(out.decode("utf-8"))


class Raphtory(interop.ScalaClassProxy):
    _classname = "com.raphtory.Raphtory$"

    algorithms = pyraphtory.algorithm.BuiltinAlgorithm("com.raphtory.algorithms")
