package com.raphtory

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table

class GraphState() extends Generic[GraphState.VertexState] {

  override def tabularise(graph: GraphPerspective): Table[GraphState.VertexState] =
    graph.select { vertex =>
      val inDeg            = vertex.inDegree
      val outDeg           = vertex.outDegree
      val degSum           = vertex.inDegree + vertex.outDegree
      val vdeletions       = vertex.numDeletions
      val vcreations       = vertex.numCreations
      val outedgedeletions = vertex.outEdges.map(edge => edge.numDeletions).sum
      val outedgecreations = vertex.outEdges.map(edge => edge.numCreations).sum

      val inedgedeletions = vertex.inEdges.map(edge => edge.numDeletions).sum
      val inedgecreations = vertex.inEdges.map(edge => edge.numCreations).sum

      val properties             = vertex.getPropertySet().size
      val propertyhistory        =
        vertex.getPropertySet().toArray.map(x => vertex.getPropertyHistory(x).size).sum
      val outedgeProperties      = vertex.outEdges.map(edge => edge.getPropertySet().size).sum
      val outedgePropertyHistory = vertex.outEdges
        .map(edge => edge.getPropertySet().toArray.map(x => edge.getPropertyHistory(x).size).sum)
        .sum

      val inedgeProperties      = vertex.inEdges.map(edge => edge.getPropertySet().size).sum
      val inedgePropertyHistory = vertex.inEdges
        .map(edge => edge.getPropertySet().toArray.map(x => edge.getPropertyHistory(x).size).sum)
        .sum

      GraphState.VertexState(
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

  case class VertexState(
      id: Any,
      inDeg: Int,
      outDeg: Int,
      degSum: Int,
      vertexDeletions: Long,
      vertexCreations: Long,
      outEdgeDeletions: Long,
      outEdgeCreations: Long,
      inEdgeDeletions: Long,
      inEdgeCreations: Long,
      properties: Int,
      propertyHistory: Int,
      outEdgeProperties: Int,
      outEdgePropertyHistory: Int,
      inEdgeProperties: Int,
      inEdgePropertyHistory: Int
  )

  def apply() = new GraphState()
}
