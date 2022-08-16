import inspect
import re
import subprocess
import json
import pandas as pd
from abc import abstractmethod
from pathlib import Path
from subprocess import PIPE, Popen
from weakref import finalize
from typing import IO, AnyStr

from py4j.java_gateway import JavaGateway, GatewayParameters
from py4j.protocol import Py4JJavaError

from pyraphtory.graph import TemporalGraph
from pyraphtory import interop, proxy


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


def join(stderr: IO[AnyStr] | None, stdout: IO[AnyStr] | None, logging: bool = False):
    if stderr and stdout:
        out = stdout.readline()
        while out:
            if logging:
                print(out)
            yield out
            out = stdout.readline()
        err = stderr.readline()
        while err:
            print(err)
            yield err
            err = stderr.readline()


class PyRaphtory(object):
    """Main python interface

    Sets up a jvm instance of raphtory and connects to it using py4j. Can be used as a context manager.
    """
    algorithms = proxy.BuiltinAlgorithm("com.raphtory.algorithms")

    def __init__(self, spout_input: Path, builder_script: Path, builder_class: str, mode: str, logging: bool = False):
        """
        Create a raphtory instance

        :param spout_input: path for input data file
        :param builder_script: path to script that contains the graph builder definition
        :param builder_class: the class name of the graph builder defined in 'builder_script'
        :param mode: one of 'batch' or 'stream' for changing the ingestion mode
        :param logging: set to True to enable verbose output during connection phase
        """
        jar_location = Path(inspect.getfile(self.__class__)).parent.parent
        jars = ":".join([str(jar) for jar in jar_location.glob('lib/*.jar')])

        self.args = ["java", "-cp", jars, "com.raphtory.python.PyRaphtory", f"--input={spout_input.absolute()}",
                     f"--py={builder_script.absolute()}", f"--builder={builder_class}", f"--mode={mode}", "--py4j"]
        self.logging = logging

    def __enter__(self):
        self.j_raphtory = Popen(
            args=self.args,
            stdout=PIPE,
            stderr=PIPE
        )

        self.java_gateway_port = None
        self.java_gateway_auth = None

        for line in self.log_lines(self.logging):
            if not line:
                break
            else:
                port_text = re.search("Started PythonGatewayServer on port ([0-9]+)", str(line))
                auth_text = re.search("PythonGatewayServer secret - ([0-9a-f]+)", str(line))
                if port_text:
                    self.java_gateway_port = int(port_text.group(1))
                if auth_text:
                    self.java_gateway_auth = auth_text.group(1).strip()

                if self.java_gateway_auth and self.java_gateway_port:
                    break

        # These two are important, if they are not closed the forked JVM will block while writing files
        self.j_raphtory.stdout.close()
        self.j_raphtory.stderr.close()

        self.j_gateway = JavaGateway(
            gateway_parameters=GatewayParameters(
                address="127.0.0.1",
                port=self.java_gateway_port,
                auth_token=self.java_gateway_auth,
                auto_field=True,
                auto_convert=True,
                eager_load=True))

        interop.set_scala_interop(self.j_gateway.entry_point.interop())

        # TODO: Best effort shutdown but if python is killed, java processes stay alive forever!
        self._finalizer = finalize(self, _kill_jvm, self.j_raphtory, self.j_gateway)
        return self

    def log_lines(self, logging: bool):
        return join(self.j_raphtory.stderr, self.j_raphtory.stdout, logging)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._finalizer()

    def open(self):
        """Create the Raphtory instance and connect to it"""
        return self.__enter__()

    def shutdown(self):
        """Shut down the Raphtory instance (this is called automatically on
        exit if the python interpreter shuts down cleanly)"""
        return self.__exit__(None, None, None)

    def graph(self):
        """Return the graph of the Raphtory instance"""
        try:
            return interop.to_python(self.j_gateway.entry_point.raphtoryGraph())
        except Py4JJavaError as err:
            print(str(err.__str__().encode('utf-8')))
            raise err
