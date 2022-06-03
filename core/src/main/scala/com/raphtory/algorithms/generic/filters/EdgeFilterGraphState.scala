package com.raphtory.algorithms.generic.filters

import com.raphtory.api.algorithm.Generic
import com.raphtory.api.algorithm.Identity
import com.raphtory.api.graphstate.GraphState
import com.raphtory.api.graphview.GraphPerspective
import com.raphtory.api.visitor.Edge

/**
  * {s} `EdgeFilter(f: (Vertex, State) => Boolean)`
  *   : Filtered view of the graph achieved by retaining edges according to a predicate function {s}`f`
  *
  *   This transforms the graph by keeping only edges for which {s}`f` returns true, where {s}`f` may depend on graph state.
  *   This fits well within a chain of algorithms as a way of pruning the graph: for example, one could first filter out edges
  *   below a certain weight before running a chosen algorithm.
  *
  * ## Parameters
  *
  * {s} `pruneNodes: Boolean=true`
  *   : if this is {s}`true` then vertices which become isolated (have no incoming or outgoing edges)
  *        after this filtering are also removed.
  *
  * ```{seealso}
  * [](com.raphtory.algorithms.generic.filters.VertexFilter)
  * [](com.raphtory.algorithms.generic.filters.EdgeQuantileFilter)
  * ```
  */

class EdgeFilterGraphState(f: (Edge, GraphState) => Boolean, pruneNodes: Boolean = true)
        extends Generic {

  override def apply(graph: GraphPerspective): graph.Graph = graph.edgeFilter(f, pruneNodes)
}

object EdgeFilterGraphState {

  def apply(f: (Edge, GraphState) => Boolean, pruneNodes: Boolean = true) =
    new EdgeFilterGraphState(f, pruneNodes)
}
