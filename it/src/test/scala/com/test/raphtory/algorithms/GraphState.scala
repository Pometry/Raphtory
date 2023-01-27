package com.test.raphtory.algorithms

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.Table

class GraphState() extends Generic {

  override def tabularise(graph: GraphPerspective): Table =
    graph
      .step { vertex =>
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

        vertex.setState("vertexID", vertex.ID)
        vertex.setState("inDegree", inDeg)
        vertex.setState("outDegree", outDeg)
        vertex.setState("degreeSum", degSum)
        vertex.setState("vertexDeletions", vdeletions)
        vertex.setState("vertexCreations", vcreations)
        vertex.setState("outedgeDeletions", outedgedeletions)
        vertex.setState("outedgeCreations", outedgecreations)
        vertex.setState("inedgeDeletions", inedgedeletions)
        vertex.setState("inedgeCreations", inedgecreations)
        vertex.setState("properties", properties)
        vertex.setState("propertyHistory", propertyhistory)
        vertex.setState("outedgeProperties", outedgeProperties)
        vertex.setState("outedgePropertyHistory", outedgePropertyHistory)
        vertex.setState("inedgeProperties", inedgeProperties)
        vertex.setState("inedgePropertyHistory", inedgePropertyHistory)

      }
      .select(
              "vertexID",
              "inDegree",
              "outDegree",
              "degreeSum",
              "vertexDeletions",
              "vertexCreations",
              "outedgeDeletions",
              "outedgeCreations",
              "inedgeDeletions",
              "inedgeCreations",
              "properties",
              "propertyHistory",
              "outedgeProperties",
              "outedgePropertyHistory",
              "inedgeProperties",
              "inedgePropertyHistory"
      )

}

object GraphState {
  def apply() = new GraphState()
}
