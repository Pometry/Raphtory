# from dill import dumps
# from pemja import findClass
#
# from pyraphtory.algo import Iterate
#
#
from abc import abstractmethod

from pyraphtory.graph import TemporalGraph


class BaseContext:

    def __init__(self, rg: TemporalGraph):
        self.rg = rg

    @abstractmethod
    def eval(self):
        pass
