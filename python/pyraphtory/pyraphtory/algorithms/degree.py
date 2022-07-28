import traceback

from pyraphtory.steps import Vertex, Step
from pyraphtory.context import BaseContext
from pyraphtory.graph import TemporalGraph


IN_DEGREE = 'inDegree'
OUT_DEGREE = 'outDegree'
DEGREE = 'degree'

class DegreeStep(Step):
    def eval(self, v: Vertex):
        v[IN_DEGREE] = v.in_degree()
        v[OUT_DEGREE] = v.out_degree()
        v[DEGREE] = v.degree()

class RaphtoryContext(BaseContext):
    def __init__(self, rg: TemporalGraph, script):
        super().__init__(rg, script)

    def eval(self):
        try:
            return self.rg.at(32674) \
                .past() \
                .step(DegreeStep()) \
                .select([IN_DEGREE, OUT_DEGREE, DEGREE]) \
                .write_to_file("/tmp/pyraphtory_degree")
        except Exception as e:
            print(str(e))
            traceback.print_exc()
