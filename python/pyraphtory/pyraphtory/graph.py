from pyraphtory.proxy import GenericScalaProxy, ScalaClassProxy, ScalaProxyBase
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
    def transform(self, algorithm):
        if isinstance(algorithm, ScalaProxyBase):
            return super().transform(algorithm)
        else:
            return algorithm(self).with_transformed_name(algorithm.__class__.__name__)

    def execute(self, algorithm):
        if isinstance(algorithm, ScalaProxyBase):
            return super().execute(algorithm)
        else:
            return algorithm.tabularise(self.transform(algorithm))


@register(name="Accumulator")
class Accumulator(GenericScalaProxy):

    def __iadd__(self, other):
        getattr(self, "$plus$eq")(other)
        return self
