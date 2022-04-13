package com.raphtory.algorithms.generic.filters

import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.algorithms.api.Identity
import com.raphtory.graph.visitor.Edge

class EdgeFilter(f: Edge => Boolean, pruneNodes: Boolean = true) extends Identity() {

  override def apply(graph: GraphPerspective): GraphPerspective =
    graph.edgeFilter(f, pruneNodes)

}

object EdgeFilter {
  def apply(f: Edge => Boolean, pruneNodes: Boolean = true) = new EdgeFilter(f, pruneNodes)
}
