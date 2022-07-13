package com.raphtory.algorithms.filters

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.visitor.Edge

/**
  * {s}`EdgeFilter(f: (Vertex, State) => Boolean)`
  *   : Filtered view of the graph achieved by retaining edges according to a predicate function {s}`f`
  *
  *   This transforms the graph by keeping only edges for which {s}`f` returns true.
  *   This fits well within a chain of algorithms as a way of pruning the graph: for example, one could first filter out edges
  *   below a certain weight before running a chosen algorithm.
  *
  * ## Parameters
  *
  * {s}`pruneNodes: Boolean=true`
  *   : if this is {s}`true` then vertices which become isolated (have no incoming or outgoing edges)
  *        after this filtering are also removed.
  *
  * ```{seealso}
  * [](com.raphtory.algorithms.filters.VertexFilter)
  * [](com.raphtory.algorithms.filters.EdgeQuantileFilter)
  * ```
  */

class EdgeFilter(f: Edge => Boolean, pruneNodes: Boolean = true) extends Generic[Any] {

  override def apply(graph: GraphPerspective): graph.Graph =
    graph.edgeFilter(f, pruneNodes)

}

object EdgeFilter {
  def apply(f: Edge => Boolean, pruneNodes: Boolean = true) = new EdgeFilter(f, pruneNodes)
}
