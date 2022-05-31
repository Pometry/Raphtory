package com.raphtory.algorithms.generic.filters

import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.algorithms.api.GraphState
import com.raphtory.algorithms.generic.NodeList
import com.raphtory.graph.visitor.Vertex

/**
  * {s} `VertexFilter(f: (Vertex, State) => Boolean)`
  *   : Filtered view of the graph achieved by retaining vertices according to a predicate function {s}`f`
  *
  *   This transforms the graph by keeping only vertices for which {s}`f` returns true, where {s}`f` may depend on graph state.
  *   This fits well within a chain of algorithms as a way of pruning the graph: for example, one could first filter out vertices
  *   below a certain degree before running a chosen algorithm.
  *
  * ```{seealso}
  * [](com.raphtory.algorithms.generic.filters.VertexFilter)
  * [](com.raphtory.algorithms.generic.filters.VertexQuantileFilter)
  * [](com.raphtory.algorithms.generic.filters.EdgeFilter)
  * ```
  */

class VertexFilterWithGraphState(f: (Vertex, GraphState) => Boolean) extends NodeList() {

  override def apply(graph: GraphPerspective): graph.Graph =
    graph.vertexFilter(f)

}

object VertexFilterGraphState {
  def apply(f: (Vertex, GraphState) => Boolean) = new VertexFilterWithGraphState(f)
}
