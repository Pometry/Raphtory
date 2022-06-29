from py4j.java_gateway import JavaGateway
import inspect
from pathlib import Path


class RaphtoryContext:
    def __init__(self):
        jar_location = Path(inspect.getfile(self.__class__)).parent.parent
        jars = list(jar_location.glob('deps/jars/*.jar'))
        print([str(jar) for jar in jars])
