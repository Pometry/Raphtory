package com.raphtory

import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.algorithms.api.Row
import com.raphtory.algorithms.api.Table
import com.raphtory.algorithms.api.algorithm.GenericAlgorithm

class TimeSeriesGraphState() extends GenericAlgorithm {

  override def tabularise(graph: GraphPerspective): Table =
    graph.select { vertex =>
      val propertyhistory =
        vertex.getPropertySet().toArray.map(x => vertex.getTimeSeriesPropertyHistory(x).size).sum

      val inEdges  = vertex.getInEdges()
      val outEdges = vertex.getOutEdges()

      val inEdgesHistory =
        inEdges.map(e => e.timeSeriesHistory().size).sum

      val outEdgesHistory =
        outEdges.map(e => e.timeSeriesHistory().size).sum

      Row(
              vertex.ID(),
              propertyhistory,
              inEdgesHistory,
              outEdgesHistory
      )
    }
}

object TimeSeriesGraphState {
  def apply() = new TimeSeriesGraphState()
}
