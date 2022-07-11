# from dill import dumps
# from pemja import findClass
#
# from pyraphtory.algo import Iterate
#
#
from abc import abstractmethod

from pyraphtory.graph import TemporalGraph


class BaseContext:

    def __init__(self, rg: TemporalGraph, script: str):
        self._rg = rg
        self.script = script

    @property
    def rg(self):
        self._rg.jvm_graph.loadPythonScript(self.script)
        return self._rg

    @abstractmethod
    def eval(self):
        pass

    @rg.setter
    def rg(self, value):
        self._rg = value
