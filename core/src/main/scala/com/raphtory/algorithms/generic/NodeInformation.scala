package com.raphtory.algorithms.generic

import com.raphtory.algorithms.api.GraphAlgorithm
import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.algorithms.api.Row
import com.raphtory.algorithms.api.Table

class NodeInformation(initialID: Long, hopsAway: Int = 1) extends GraphAlgorithm {

  case class Node(label: String, metadata: NodeData, edges: Array[EdgeInfo])
  case class NodeData(id: String)
  case class EdgeInfo(source: String, target: String, metadata: EdgeData)
  case class EdgeData(value: Int)

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
        val vertexID                         = vertex.ID()
        val name                             = vertex.name()
        val involved: Boolean                = vertex.getStateOrElse("vertexInvolved", false)
        val edgeInformation: Array[EdgeInfo] = vertex
          .getEdges()
          .map { edge =>
            EdgeInfo(
                    edge.src().toString,
                    edge.dst().toString,
                    EdgeData(edge.weight(weightProperty = "Character Co-occurence", 0))
            )
          }
          .toArray

        Row(
                involved,
                Node(
                        name,
                        NodeData(vertexID.toString),
                        edgeInformation
                )
        )
      }
      .filter(row => row.getBool(0))
}

object NodeInformation {
  def apply(initialID: Long, hopsAway: Int = 1) = new NodeInformation(initialID, hopsAway)
}
