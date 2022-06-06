package com.raphtory.algorithms.generic.filters

import com.raphtory.algorithms.generic.NodeList
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.visitor.Vertex

/**
  * {s} `VertexFilter(f: Vertex => Boolean)`
  *   : Filtered view of the graph achieved by retaining vertices according to a predicate function {s}`f`
  *
  *   This transforms the graph by keeping only vertices for which {s}`f` returns true. This fits well within a chain of
  *   algorithms as a way of pruning the graph: for example, one could first filter out vertices below a certain degree
  *   before running a chosen algorithm.
  *
  * ```{seealso}
  * [](com.raphtory.algorithms.generic.filters.VertexFilterGraphState)
  * [](com.raphtory.algorithms.generic.filters.VertexQuantileFilter)
  * [](com.raphtory.algorithms.generic.filters.EdgeFilter)
  * ```
  */
class VertexFilter(f: Vertex => Boolean) extends NodeList() {

  override def apply(graph: GraphPerspective): graph.Graph =
    graph.vertexFilter(f)

}

object VertexFilter {
  def apply(f: Vertex => Boolean) = new VertexFilter(f)
}
