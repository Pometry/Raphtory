from pyraphtory.proxy import GenericScalaProxy, ScalaClassProxy
from pyraphtory.interop import register, logger, to_jvm, find_class
import pandas as pd
import json


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
            for name, item in zip(cols, row):
                jsonRow[name] = item
            jsonRow.pop('row')
            newJson.append(jsonRow)
        return pd.DataFrame(newJson)


class Row(ScalaClassProxy):
    _classname = "com.raphtory.api.analysis.table.Row"


class PropertyMergeStrategy(ScalaClassProxy):
    _classname = "com.raphtory.api.analysis.visitor.PropertyMergeStrategy"


@register(name="TemporalGraph")
class TemporalGraph(GenericScalaProxy):
    pass


@register(name="Accumulator")
class Accumulator(GenericScalaProxy):

    def __iadd__(self, other):
        getattr(self, "$plus$eq")(other)
        return self
