"""Python wrappers for graph api"""

from __future__ import annotations
from pyraphtory.interop import register, logger, GenericScalaProxy, ScalaClassProxy
from pyraphtory.api.input import Properties, ImmutableString, Type
from pyraphtory.api.algorithm import ScalaAlgorithm
from pyraphtory.interop._interop import ScalaProxyBase


@register(name="Perspective")
class Perspective(GenericScalaProxy):
    _classname = "com.raphtory.api.time.Perspective"


class Alignment(ScalaClassProxy):
    _classname = "com.raphtory.api.analysis.graphview.Alignment"


class PropertyMergeStrategy(ScalaClassProxy):
    _classname = "com.raphtory.api.analysis.visitor.PropertyMergeStrategy"


@register(name="Graph")
class Graph(GenericScalaProxy):
    _classname = "com.raphtory.api.input.Graph"

    def add_vertex(self, update_time: int, src_id: int | str, properties: list[input.Property] = (), vertex_type: str = "", secondary_index: int=None,time_format="yyyy[-MM[-dd[ HH[:mm[:ss[.SSS]]]]]]"):
        """Adds a new vertex to the graph or updates an existing vertex

           :param update_time: timestamp for vertex update
           :param src_id: ID of vertex to add/update
           :param properties: Optionally specify vertex properties for the update (see :py:mod:`~pyraphtory.input` for the
                              available property types)
           :param vertex_type: Optionally specify a type for the vertex
           :param secondary_index: Optionally specify a secondary index that is used to determine the order
                                   of updates with the same `update_time`
           :param time_format: Optionally set the format if providing a datetime String for update_time.
                                   By default this is yyyy-MM-dd HH:mm:ss.SSS
        """
        if secondary_index is None:
            secondary_index = self.index()
        time = update_time
        if isinstance(update_time, str):
            time = self.parse_datetime(time,time_format)

        if isinstance(src_id, str):
            properties += (ImmutableString("name", src_id),)  # comma makes this a tuple and is apparently the fastest way to do things
            super().add_vertex(time, self.assign_id(src_id), Properties(*properties), Type(vertex_type),secondary_index)
        else:
            super().add_vertex(time, src_id, Properties(*properties), Type(vertex_type), secondary_index)

    def add_edge(self, update_time: int, src_id: int | str, dst_id: int | str, properties: list[input.Property] = (),
                 edge_type: str = "", secondary_index: int = None,time_format="yyyy[-MM[-dd[ HH[:mm[:ss[.SSS]]]]]]"):
        """Adds a new edge to the graph or updates an existing edge

        :param update_time: timestamp for edge update
        :param src_id: ID of source vertex of the edge
        :param dst_id: ID of destination vertex of the edge
        :param properties: edge properties for the update (see :py:mod:`~pyraphtory.input` for the available property types)
        :param edge_type: specify a type for the edge
        :param secondary_index: Optionally specify a secondary index that is used to determine the order
                                of updates with the same `update_time`
        :param time_format: Optionally set the format if providing a datetime String for update_time.
                                By default this is yyyy-MM-dd HH:mm:ss.SSS
        """
        if secondary_index is None:
            secondary_index = self.index()
        time = update_time
        if isinstance(update_time, str):
            time = self.parse_datetime(time,time_format)
        source = src_id
        destination = dst_id
        if isinstance(src_id, str):
            source = self.assign_id(src_id)
        if isinstance(dst_id, str):
            destination = self.assign_id(dst_id)
        super().add_edge(time, source, destination, Properties(*properties), Type(edge_type), secondary_index)


@register(name="TemporalGraph")
class TemporalGraph(Graph):
    _classname = "com.raphtory.api.analysis.graphview.TemporalGraph"

    def transform(self, algorithm: algorithm.PyAlgorithm | algorithm.ScalaAlgorithm):
        """Apply an algorithm to the graph

        :param algorithm: Algorithm to apply to the graph
        :return: Transformed graph

        .. note::
           `transform` keeps track of the name of the applied algorithm and clears the message queues at the end of the algorithm
        """
        if isinstance(algorithm, ScalaProxyBase):
            return super().transform(algorithm)
        else:
            return algorithm(self).clear_messages().with_transformed_name(algorithm.name())

    def execute(self, algorithm):
        """Run an algorithm on the graph and return results using the `tabularise` method of the algorithm

        :param algorithm: Algorithm to run
        :return: Table with algorithm results

        .. note::

           `execute` keeps track of the name of the applied algorithm
        """
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



