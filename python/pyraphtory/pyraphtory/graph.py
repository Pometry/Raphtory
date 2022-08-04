import traceback
from typing import List

import cloudpickle as pickle

from pyraphtory.steps import Iterate, Step, State, StepState, GlobalSelect
from pyraphtory.proxy import GenericScalaProxy
from pyraphtory.interop import register, logger, to_jvm, find_class
import pandas as pd
import json
import inspect


class ProgressTracker(GenericScalaProxy):
    _classname = "com.raphtory.api.querytracker.QueryProgressTracker"

    def inner_tracker(self):
        logger.trace("Progress tracker inner tracker returned")
        return to_jvm(self)


@register(name="Table")
class Table(GenericScalaProxy):
    def write_to_dataframe(self, cols):
        sink = find_class("com.raphtory.sinks.LocalQueueSink").apply()
        self.write_to(sink).wait_for_job()
        res = sink.results()
        newJson = []
        for r in res:
            jsonRow = json.loads(r)
            row = jsonRow['row']
            for i, item in enumerate(row):
                if i == 0:
                    jsonRow['name'] = row[0]
                    continue
                jsonRow[cols[i-1]] = row[i]
            jsonRow.pop('row')
            newJson.append(jsonRow)
        return pd.DataFrame(newJson)


@register(name="TemporalGraph")
class TemporalGraph(GenericScalaProxy):

    def set_global_state(self, fun):
        state_bytes = pickle.dumps(State(fun))
        return self.python_set_global_state(state_bytes)

    def step(self, fun):
        spec = inspect.getfullargspec(fun)
        if len(spec.args) == 1:
            return self.python_step(pickle.dumps(Step(fun)))
        elif len(spec.args) == 2:
            return self.python_step_state(pickle.dumps(StepState(fun)))
        else:
            raise ValueError("Expected function with one or two arguments")

    def iterate(self, i: Iterate):
        logger.trace("iterate called")
        iterate_bytes = pickle.dumps(i)
        return self.python_iterate(iterate_bytes, i.iterations, i.execute_messaged_only)

    def select(self, columns: List[str]):
        logger.trace("select called")
        return self.python_select(columns)

    def select_state(self, columns: List[str]):
        return self.python_select_state(columns)

    def step_state(self, ssb: StepState):
        logger.trace("step_state called")
        step_state_bytes = pickle.dumps(ssb)
        return self.python_step_state(step_state_bytes)

    def global_select(self, fun):
        logger.trace("global_select called")
        global_select_bytes = pickle.dumps(GlobalSelect(fun))
        return self.python_global_select(global_select_bytes)


@register(name="Accumulator")
class Accumulator(GenericScalaProxy):
    def __iadd__(self, other):
        self.__getattr__("+=")(other)
