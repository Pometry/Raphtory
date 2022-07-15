# from dill import dumps
# from pemja import findClass
#
# from pyraphtory.algo import Iterate
#
#
from abc import abstractmethod

from pyraphtory.graph import TemporalGraph


class BaseContext(object):

    def __init__(self, rg: TemporalGraph, script: str):
        self._rg = rg
        self.script = script

    @property
    def rg(self):
        g = self._rg.jvm_graph.loadPythonScript(self.script)
        return TemporalGraph(g)

    @abstractmethod
    def eval(self):
        pass

    @rg.setter
    def rg(self, value):
        self._rg = value
