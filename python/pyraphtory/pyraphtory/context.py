from __future__ import annotations

from typing import AnyStr

from py4j.java_gateway import JavaGateway, GatewayParameters
from py4j.protocol import Py4JJavaError
import inspect
from pathlib import Path
from subprocess import PIPE, Popen
import re

from typing.io import IO

from pyraphtory.extra import ConnectedComponents, Sink
from pyraphtory.graph import TemporalGraphConnection


def join(stderr: IO[AnyStr] | None, stdout: IO[AnyStr] | None):
    if stderr and stdout:
        out = stdout.readline()
        while out:
            yield out
            out = stdout.readline()
        err = stderr.readline()
        while err:
            yield err
            err = stderr.readline()


class RaphtoryContext:

    def __init__(self, input_file: Path):
        jar_location = Path(inspect.getfile(self.__class__)).parent.parent
        jars = ":".join([str(jar) for jar in jar_location.glob('deps/jars/*.jar')])

        self.j_raphtory = Popen(
            args=["java", "-cp", jars, "com.raphtory.python.RaphtoryLocal", "--input-file", str(input_file)],
            stdout=PIPE,
            stderr=PIPE
        )

        self.java_gateway_port = None
        self.java_gateway_auth = None

        for line in join(self.j_raphtory.stderr, self.j_raphtory.stdout):
            if not line:
                break
            else:
                print(line)
                port_text = re.search("Started PythonGatewayServer on port ([0-9]+)", str(line))
                auth_text = re.search("PythonGatewayServer secret - ([0-9a-f]+)", str(line))
                if port_text:
                    self.java_gateway_port = int(port_text.group(1))
                if auth_text:
                    self.java_gateway_auth = auth_text.group(1).strip()

                if self.java_gateway_auth and self.java_gateway_port:
                    break

    def __enter__(self):
        self.j_gateway = JavaGateway(
            gateway_parameters=GatewayParameters(
                address="127.0.0.1",
                port=self.java_gateway_port,
                auth_token=self.java_gateway_auth,
                auto_field=True,
                auto_convert=True,
                eager_load=True))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.j_raphtory.kill()
        finally:
            pass

        try:
            self.j_gateway.close()
        finally:
            pass

    def graph(self):
        try:
            jvm_graph = self.j_gateway.entry_point.raphtory_graph()
            return TemporalGraphConnection(jvm_graph)
        except Py4JJavaError as err:
            print(str(err.__str__().encode('utf-8')))
            raise err

    def connected_components(self):
        cc_algo = self.j_gateway.entry_point.connected_components()
        return ConnectedComponents(cc_algo)

    def file_sink(self, path: Path):
        jvm_sink = self.j_gateway.entry_point.file_sink(str(path))
        return Sink(jvm_sink)


if __name__ == '__main__':
    with RaphtoryContext(input_file=Path('/tmp/lotr.csv')) as rc:
        graph = rc.graph()
        graph \
            .at(32674) \
            .past() \
            .execute(rc.connected_components()) \
            .write_to(rc.file_sink(Path('/tmp/pyraphtory'))) \
            .wait_for_job()
