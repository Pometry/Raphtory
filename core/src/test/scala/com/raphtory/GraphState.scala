package com.raphtory

import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.algorithms.api.Row
import com.raphtory.algorithms.api.Table
import com.raphtory.algorithms.api.algorithm.GenericAlgorithm

class GraphState() extends GenericAlgorithm {

  override def tabularise[G <: GraphPerspective[G]](graph: G): Table =
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
              vertex.ID(),
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
