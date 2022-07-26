import inspect
import re
import subprocess
from abc import abstractmethod
from pathlib import Path
from subprocess import PIPE, Popen
from threading import Thread
from typing import IO, AnyStr

from py4j.java_gateway import JavaGateway, GatewayParameters
from py4j.protocol import Py4JJavaError

from pyraphtory.graph import TemporalGraph

class PyRaphtory(object):

    def connected_components(self):
        return self.j_gateway.entry_point.connectedComponents()