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


class BaseContext(object):

    def __init__(self, rg, script: str):
        interop.logger.trace("Constructing context")
        self._rg = interop.to_python(rg)
        self.script = script

    @property
    def rg(self):
        return self._rg.load_python_script(self.script)

    @abstractmethod
    def eval(self):
        pass

    def eval_from_jvm(self):
        return interop.to_jvm(self.eval())

    @rg.setter
    def rg(self, value):
        self._rg = interop.to_python(value)


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


class PyRaphtory(object):

    """Main python interface

    Sets up a jvm instance of raphtory and connects to it using py4j. Can be used as a context manager.
    """

    def __init__(self, logging: bool = False):
        """
        Create a raphtory instance

        To customise jvm startup options use the 'PYRAPHTORY_JVM_ARGS' environment variable.

        PYRAPTHORY_JAR_LOCATION is the absolute path to a Raphtory jar file

        :param logging: set to True to enable verbose output during connection phase
        """
        env_jar_location = os.environ.get("PYRAPTHORY_JAR_LOCATION", "")
        env_jar_glob_lookup = os.environ.get("PYRAPTHORY_JAR_GLOB_LOOKUP", '*.jar')
        if env_jar_location != "":
            jar_location = Path(env_jar_location)
        else:
            jar_location = Path(inspect.getfile(self.__class__)).parent.parent / 'lib'
        jars = ":".join([str(jar) for jar in jar_location.glob(env_jar_glob_lookup)])
        java_args = os.environ.get("PYRAPTHORY_JVM_ARGS", "")

        # if jars is empty, then download it
        if not jars:
            jars = fileutils.download_raphtory(__version__,
                                               str(Path(inspect.getfile(self.__class__)).parent.parent / 'lib'))

        if java_args:
            self.args = ["java", java_args, "-cp", jars, "com.raphtory.python.PyRaphtory", "--parentID", str(os.getpid())]
        else:
            self.args = ["java", "-cp", jars, "com.raphtory.python.PyRaphtory", "--parentID", str(os.getpid())]
        self.logging = logging

    def __enter__(self):
        j_raphtory = Popen(
            args=self.args,
            stdout=PIPE,
            stderr=PIPE
        )

        stderr_logging = Thread(target=print_output, args=(j_raphtory.stderr, sys.stderr))
        stderr_logging.daemon = True
        stderr_logging.start()

        java_gateway_port = None
        java_gateway_auth = None

        for line in read_output(j_raphtory.stdout, self.logging):
                port_text = re.search("Port: ([0-9]+)", str(line))
                auth_text = re.search("Secret: ([0-9a-f]+)", str(line))
                if port_text:
                    java_gateway_port = int(port_text.group(1))
                if auth_text:
                    java_gateway_auth = auth_text.group(1).strip()

                if java_gateway_auth and java_gateway_port:
                    break
        else:
            raise RuntimeError("Could not find connection details")

        if self.logging:
            stdout_logging = Thread(target=print_output, args=(j_raphtory.stdout,))
            stdout_logging.daemon = True
            stdout_logging.start()
        else:
            j_raphtory.stdout.close()

        j_gateway = JavaGateway(
            gateway_parameters=GatewayParameters(
                address="127.0.0.1",
                port=java_gateway_port,
                auth_token=java_gateway_auth,
                auto_field=True,
                auto_convert=True,
                eager_load=True))

        interop.set_scala_interop(j_gateway.entry_point)
        context = interop.to_python(interop.find_class("com.raphtory.Raphtory"))
        context._j_raphtory = j_raphtory
        context._j_gateway = j_gateway
        context._finalizer = finalize(context, _kill_jvm, j_raphtory, j_gateway)
        self._finalizer = context._finalizer
        return context

    def open(self):
        """Create the Raphtory instance and connect to it"""
        return self.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._finalizer()


class Raphtory(interop.GenericScalaProxy):
    _classname = "com.raphtory.Raphtory$"

    algorithms = pyraphtory.algorithm.BuiltinAlgorithm("com.raphtory.algorithms")

    def shutdown(self):
        """Shut down the Raphtory instance (this is called automatically on
        exit if the python interpreter shuts down cleanly)"""
        self._finalizer()
