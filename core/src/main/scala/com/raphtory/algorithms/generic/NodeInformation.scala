package com.raphtory.algorithms.generic

import com.raphtory.algorithms.api.GraphAlgorithm
import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.algorithms.api.Row
import com.raphtory.algorithms.api.Table

class NodeInformation(initialID: Long, hopsAway: Int = 1) extends GraphAlgorithm {

  case class VertexInformation(
      id: String,
      name: String,
      outgoingEdges: Array[Long],
      incomingEdges: Array[Long]
  )

  override def apply(graph: GraphPerspective): GraphPerspective =
    graph
      .step { vertex =>
        if (vertex.ID() == initialID) {
          vertex.setState("vertexInvolved", true)
          vertex.messageAllNeighbours(true)
        }
      }
      .iterate(
              { vertex =>
                vertex.setState("vertexInvolved", true)
                if (hopsAway > 1)
                  vertex.messageAllNeighbours(true)
              },
              hopsAway,
              true
      )

  override def tabularise(graph: GraphPerspective): Table =
    graph
      .select { vertex =>
        val vertexID          = vertex.ID().toString
        val name              = vertex.name()
        val involved: Boolean = vertex.getStateOrElse("vertexInvolved", false)
        val outgoingEdges     = vertex.getOutEdges().map(edge => edge.ID().toString.toLong).toArray
        val incomingEdges     = vertex.getInEdges().map(edge => edge.ID().toString.toLong).toArray

        Row(
                involved,
                VertexInformation(vertexID, name, outgoingEdges, incomingEdges)
        )
      }
      .filter(row => row.getBool(0))
}

object NodeInformation {
  def apply(initialID: Long, hopsAway: Int = 1) = new NodeInformation(initialID, hopsAway)
}
