package com.raphtory

import com.raphtory.api.algorithm.Generic
import com.raphtory.api.graphview.GraphPerspective
import com.raphtory.api.table.Row
import com.raphtory.api.table.Table

class TimeSeriesGraphState() extends Generic {

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
              vertex.ID,
              propertyhistory,
              inEdgesHistory,
              outEdgesHistory
      )
    }
}

object TimeSeriesGraphState {
  def apply() = new TimeSeriesGraphState()
}
