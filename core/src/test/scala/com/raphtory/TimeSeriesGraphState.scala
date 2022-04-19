package com.raphtory

import com.raphtory.algorithms.api.GraphAlgorithm
import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.algorithms.api.Row
import com.raphtory.algorithms.api.Table

class TimeSeriesGraphState() extends GraphAlgorithm {

  override def tabularise(graph: GraphPerspective): Table =
    graph.select { vertex =>
      val propertyhistory =
        vertex.getPropertySet().toArray.map(x => vertex.getTimeSeriesPropertyHistory(x).size).sum
      val inEdges         = vertex.getInEdges()
      val inEdgesHistory  =
        inEdges.map(e => e.timeSeriesHistory().size).sum
      Row(
              vertex.ID(),
              propertyhistory,
              inEdgesHistory
      )
    }
}

object TimeSeriesGraphState {
  def apply() = new TimeSeriesGraphState()
}
