from py4j.java_gateway import JavaGateway, GatewayParameters
import inspect
from pathlib import Path
from subprocess import PIPE, Popen
import re


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

        while True:
            line = self.j_raphtory.stdout.readline()
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

        self.j_gateway = JavaGateway(
            gateway_parameters=GatewayParameters(
                address="127.0.0.1",
                port=self.java_gateway_port,
                auth_token=self.java_gateway_auth,
                auto_field=True,
                auto_convert=True,
                eager_load=True))


if __name__ == '__main__':
    # Execute when the module is not initialized from an import statement.
    rc = RaphtoryContext(input_file=Path('/tmp/lotr.csv'))
    print(rc.java_gateway_port)
    jvm = rc.j_gateway.jvm
    l = jvm.java.util.ArrayList()
    l.append(10)
    l.append(1)
    jvm.java.util.Collections.sort(l)
    print(l)
