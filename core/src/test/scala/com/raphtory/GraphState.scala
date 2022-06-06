package com.raphtory

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table

class GraphState() extends Generic {

  override def tabularise(graph: GraphPerspective): Table =
    graph.select { vertex =>
      val inDeg            = vertex.inDegree
      val outDeg           = vertex.outDegree
      val degSum           = vertex.inDegree + vertex.outDegree
      val vdeletions       = vertex.numDeletions
      val vcreations       = vertex.numCreations
      val outedgedeletions = vertex.getOutEdges().map(edge => edge.numDeletions).sum
      val outedgecreations = vertex.getOutEdges().map(edge => edge.numCreations).sum

      val inedgedeletions = vertex.getInEdges().map(edge => edge.numDeletions).sum
      val inedgecreations = vertex.getInEdges().map(edge => edge.numCreations).sum

      val properties             = vertex.getPropertySet().size
      val propertyhistory        =
        vertex.getPropertySet().toArray.map(x => vertex.getPropertyHistory(x).size).sum
      val outedgeProperties      = vertex.getOutEdges().map(edge => edge.getPropertySet().size).sum
      val outedgePropertyHistory = vertex
        .getOutEdges()
        .map(edge => edge.getPropertySet().toArray.map(x => edge.getPropertyHistory(x).size).sum)
        .sum

      val inedgeProperties      = vertex.getInEdges().map(edge => edge.getPropertySet().size).sum
      val inedgePropertyHistory = vertex
        .getInEdges()
        .map(edge => edge.getPropertySet().toArray.map(x => edge.getPropertyHistory(x).size).sum)
        .sum

      Row(
              vertex.ID,
              inDeg,
              outDeg,
              degSum,
              vdeletions,
              vcreations,
              outedgedeletions,
              outedgecreations,
              inedgedeletions,
              inedgecreations,
              properties,
              propertyhistory,
              outedgeProperties,
              outedgePropertyHistory,
              inedgeProperties,
              inedgePropertyHistory
      )
    }
}

object GraphState {
  def apply() = new GraphState()
}
