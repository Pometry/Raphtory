package com.raphtory

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table

class TimeSeriesGraphState() extends Generic[Row] {

  override def tabularise(graph: GraphPerspective): Table[Row] =
    graph.select { vertex =>
      val propertyhistory =
        vertex.getPropertySet().toArray.map(x => vertex.getTimeSeriesPropertyHistory(x).size).sum

      val inEdges  = vertex.inEdges
      val outEdges = vertex.outEdges

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
