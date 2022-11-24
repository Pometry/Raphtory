from pyraphtory.interop import register, logger, to_jvm, find_class, ScalaProxyBase, GenericScalaProxy, ScalaClassProxy
from pyraphtory.input import Properties,ImmutableProperty,Type
from pyraphtory.scala.implicits.bounded import Bounded
import pandas as pd
import json


class ProgressTracker(GenericScalaProxy):
    _classname = "com.raphtory.api.querytracker.QueryProgressTracker"

    def inner_tracker(self):
        logger.trace("Progress tracker inner tracker returned")
        return to_jvm(self)


@register(name="Table")
class Table(GenericScalaProxy):
    _classname = "com.raphtory.api.analysis.table.Table"

    def to_df(self, cols):
        rows = []
        for res in self.get():
            timestamp = res.perspective().timestamp()
            window = res.perspective().window()
            if(window!=None):
                for r in res.rows():
                    rows.append((timestamp, window, *r.get_values()))
                return pd.DataFrame.from_records(rows, columns=('timestamp', 'window', *cols))
            else:
                for r in res.rows():
                    rows.append((timestamp, *r.get_values()))
                return pd.DataFrame.from_records(rows, columns=('timestamp', *cols))

class Row(ScalaClassProxy):
    _classname = "com.raphtory.api.analysis.table.Row"


class PropertyMergeStrategy(ScalaClassProxy):
    _classname = "com.raphtory.api.analysis.visitor.PropertyMergeStrategy"


@register(name="TemporalGraph")
class TemporalGraph(GenericScalaProxy):
    _classname = "com.raphtory.api.analysis.graphview.TemporalGraph"
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

    def addVertex(self,update_time,src_id,properties=Properties(),vertex_type=None,secondary_index=None):
        if secondary_index is None:
            secondary_index = self.index()
        if vertex_type is None:
            vertex_type=Type("")
        if isinstance(src_id, str):
            properties= properties.add_property(ImmutableProperty("name", src_id))
            self.add_vertex(update_time,self.assign_id(src_id),properties,vertex_type,secondary_index)
        else:
            self.add_vertex(update_time,src_id,properties,vertex_type,secondary_index)


class DeployedTemporalGraph(TemporalGraph):
    _classname = "com.raphtory.api.analysis.graphview.PyDeployedTemporalGraph"

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.debug("Graph closed using context manager")
        self.close()

@register(name="Accumulator")
class Accumulator(GenericScalaProxy):
    _classname = "com.raphtory.api.analysis.graphstate.Accumulator"

    def __iadd__(self, other):
        self._plus_eq(other)
        return self
