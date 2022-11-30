import os
import re
import sys
from subprocess import Popen, PIPE
from threading import Thread
from typing import AnyStr
from weakref import finalize

import pyraphtory_jvm.jre
from py4j.java_gateway import JavaGateway, GatewayParameters
import jpype
import jpype.imports

from typing.io import IO

import pyraphtory._config


def _kill_jvm(j_raphtory, j_gateway):
    try:
        j_gateway.close()
    finally:
        pass
    try:
        j_raphtory.kill()
    finally:
        pass


def print_output(input_stream: IO[AnyStr], output_stream=sys.stdout):
    out = input_stream.readline()
    while out:
        output_stream.write(out.decode("utf-8"))
        out = input_stream.readline()


def read_output(stream: IO[AnyStr]):
    out = stream.readline()
    while out:
        yield out
        out = stream.readline()

def check_raphtory_logging_env():
    log_level = os.getenv('RAPHTORY_CORE_LOG')
    if log_level is None:
        os.environ["RAPHTORY_CORE_LOG"] = "ERROR"

class Py4JConnection:
    def __init__(self):

        check_raphtory_logging_env()
        java = pyraphtory_jvm.jre.get_local_java_loc()

        if pyraphtory._config.java_args:
            args = [java, pyraphtory._config.java_args, "-cp", pyraphtory._config.jars,
                    "com.raphtory.python.PyRaphtory", "--parentID", str(os.getpid())]
        else:
            args = [java, "-cp", pyraphtory._config.jars, "com.raphtory.python.PyRaphtory",
                    "--parentID", str(os.getpid())]
        j_raphtory = Popen(
            args=args,
            stdout=PIPE,
            stderr=PIPE
        )

        stderr_logging = Thread(target=print_output, args=(j_raphtory.stderr, sys.stderr))
        stderr_logging.daemon = True
        stderr_logging.start()

        java_gateway_port = None
        java_gateway_auth = None

        for line in read_output(j_raphtory.stdout):
            port_text = re.search("Port: ([0-9]+)", str(line))
            auth_text = re.search("Secret: ([0-9a-f]+)", str(line))
            if port_text:
                java_gateway_port = int(port_text.group(1))
            elif auth_text:
                java_gateway_auth = auth_text.group(1).strip()
            else:
                sys.stdout.write(line.decode("utf-8"))

            if java_gateway_auth and java_gateway_port:
                break
        else:
            raise RuntimeError("Could not find connection details")

        stdout_logging = Thread(target=print_output, args=(j_raphtory.stdout,))
        stdout_logging.daemon = True
        stdout_logging.start()

        j_gateway = JavaGateway(
            gateway_parameters=GatewayParameters(
                address="127.0.0.1",
                port=java_gateway_port,
                auth_token=java_gateway_auth,
                auto_field=True,
                auto_convert=True,
                eager_load=True))

        self.j_gateway = j_gateway
        self.stderr_logging = stderr_logging
        self.stdout_logging = stdout_logging
        self.j_raphtory = j_raphtory
        self._finalizer = finalize(self, _kill_jvm, j_raphtory, j_gateway)

