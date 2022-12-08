from pyraphtory.interop import register, logger, to_jvm, find_class, ScalaProxyBase, GenericScalaProxy, ScalaClassProxy
from pyraphtory.input import Properties, ImmutableString, Type
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
        columns = ()
        for res in self.get():
            timestamp = res.perspective().timestamp()
            window = res.perspective().window()
            if (window != None):
                columns = ('timestamp', 'window', *cols)
                for r in res.rows():
                    window_size = window.get().size()
                    rows.append((timestamp, window_size, *r.get_values()))
            else:
                columns = ('timestamp', *cols)
                for r in res.rows():
                    rows.append((timestamp, *r.get_values()))
        return pd.DataFrame.from_records(rows, columns=columns)


class Row(ScalaClassProxy):
    _classname = "com.raphtory.api.analysis.table.Row"


class PropertyMergeStrategy(ScalaClassProxy):
    _classname = "com.raphtory.api.analysis.visitor.PropertyMergeStrategy"


@register(name="Graph")
class Graph(GenericScalaProxy):
    _classname = "com.raphtory.api.input.Graph"

    def add_vertex(self, update_time, src_id, properties=(), vertex_type="", secondary_index=None):
        if secondary_index is None:
            secondary_index = self.index()

        if isinstance(src_id, str):
            properties += (ImmutableString("name", src_id),)  # comma makes this a tuple and is apparently the fastest way to do things
            super().add_vertex(update_time, self.assign_id(src_id), Properties(*properties), Type(vertex_type),
                               secondary_index)
        else:
            super().add_vertex(update_time, src_id, Properties(*properties), Type(vertex_type), secondary_index)

    def add_edge(self, update_time, src_id, dst_id, properties=(), edge_type="", secondary_index=None):
        if secondary_index is None:
            secondary_index = self.index()
        source = src_id
        destination = dst_id
        if isinstance(src_id, str):
            source = self.assign_id(src_id)
        if isinstance(dst_id, str):
            destination = self.assign_id(dst_id)
        super().add_edge(update_time, source, destination, Properties(*properties), Type(edge_type), secondary_index)


@register(name="TemporalGraph")
class TemporalGraph(Graph):
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
