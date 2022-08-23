package com.raphtory.algorithms.generic

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphstate.Counter
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table

object LargestConnectedComponent extends Generic {

  override def apply(graph: GraphPerspective): graph.Graph =
    ConnectedComponents
      .apply(graph)
      .setGlobalState(state => state.newCounter[Long]("components", retainState = true))
      .step { (vertex, state) =>
        state("components") += (vertex.getState[Long]("cclabel"), 1)
      }

  override def tabularise(graph: GraphPerspective): Table =
    graph.globalSelect(state => Row(state("components").value.asInstanceOf[Counter[Long]].largest._2))
}
