package com.raphtory.algorithms.filters

import com.raphtory.api.analysis.graphstate.Counter
import com.raphtory.algorithms.generic.{ConnectedComponents, NodeList}
import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.visitor.Vertex

/**
  * {s}`LargestConnectedComponentFilter()`
  *   : Filtered view of the graph achieved by retaining vertices of the largest connected component.
  *
  *   This runs the connected component algorithm, keeping a global count of component sizes. This then
  *   selects vertices of the largest component.
  *
  */
class LargestConnectedComponentFilter extends NodeList {

  override def apply(graph: GraphPerspective): graph.Graph = ConnectedComponents
    .apply(graph)
    .setGlobalState(state => state.newCounter[Long]("components", retainState = true))
    .step { (vertex, state) =>
      state("components") += (vertex.getState[Long]("cclabel"), 1)
    }

    .vertexFilter { (vertex, state) =>
      val vertexProperty = vertex.getState[Long]("cclabel").toFloat
      vertexProperty == state("components").value.asInstanceOf[Counter[Long]].largest._1
    }
}

object LargestConnectedComponentFilter {
  def apply() = new LargestConnectedComponentFilter
}

