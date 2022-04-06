package com.raphtory

import com.raphtory.algorithms.api.GraphAlgorithm
import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.algorithms.api.Row
import com.raphtory.algorithms.api.Table

class TimeSeriesGraphState() extends GraphAlgorithm {

  override def tabularise(graph: GraphPerspective): Table =
    graph.select { vertex =>
      val inDeg  = vertex.inDegree
      val outDeg = vertex.outDegree
      val degSum = vertex.inDegree + vertex.outDegree

      val properties             = vertex.getPropertySet().size
      val propertyhistory        =
        vertex.getPropertySet().toArray.map(x => vertex.getTimeSeriesPropertyHistory(x).size).sum
      val outedgeProperties      = vertex.getOutEdges().map(edge => edge.getPropertySet().size).sum
      val outedgePropertyHistory = vertex
        .getOutEdges()
        .map(edge =>
          edge.getPropertySet().toArray.map(x => edge.getTimeSeriesPropertyHistory(x).size).sum
        )
        .sum

      val inedgeProperties      = vertex.getInEdges().map(edge => edge.getPropertySet().size).sum
      val inedgePropertyHistory = vertex
        .getInEdges()
        .map(edge =>
          edge.getPropertySet().toArray.map(x => edge.getTimeSeriesPropertyHistory(x).size).sum
        )
        .sum

      Row(
              vertex.ID(),
              inDeg,
              outDeg,
              degSum,
              properties,
              propertyhistory,
              outedgeProperties,
              outedgePropertyHistory,
              inedgeProperties,
              inedgePropertyHistory
      )
    }
}

object TimeSeriesGraphState {
  def apply() = new TimeSeriesGraphState()
}
