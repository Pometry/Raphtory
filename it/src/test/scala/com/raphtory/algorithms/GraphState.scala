package com.test.raphtory.algorithms

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.{KeyPair, Row, Table}

class GraphState() extends Generic {

  override def tabularise(graph: GraphPerspective): Table =
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

      Row(
        KeyPair("vertexID", vertex.ID),
        KeyPair("inDegree",inDeg),
        KeyPair("outDegree", outDeg),
        KeyPair("degreeSum",degSum),
        KeyPair("vertexDeletions", vdeletions),
        KeyPair("vertexCreations", vcreations),
        KeyPair("outedgeDeletions", outedgedeletions),
        KeyPair("outedgeCreations", outedgecreations),
        KeyPair("inedgeDeletions", inedgedeletions),
        KeyPair("inedgeCreations",inedgecreations),
        KeyPair("properties", properties),
        KeyPair("propertyHistory", propertyhistory),
        KeyPair("outedgeProperties",outedgeProperties),
        KeyPair("outedgePropertyHistory",outedgePropertyHistory),
        KeyPair("inedgeProperties",inedgeProperties),
        KeyPair("inedgePropertyHistory",inedgePropertyHistory)
      )
    }
}

object GraphState {
  def apply() = new GraphState()
}
