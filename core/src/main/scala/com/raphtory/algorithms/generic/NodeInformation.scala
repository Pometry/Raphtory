package com.raphtory.algorithms.generic

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.Table
import com.raphtory.internals.communication.SchemaProviderInstances._

/**
  * {s}`NodeInformation(initialID: Long, hopsAway: Int = 1)`
  *   : Finds information about node and neighbours of node X hops away (default = 1).
  *
  *  ## Usage
  *     Used with JsonOutputRunner.scala (located in the lotrTopic example) and JsonFormat.scala to output node information in Json format.
  *
  *  ## Parameters
  *     {s}`initialID: Long`
  *       : initial vertex ID that user inputs
  *
  *     {s}`hopAway: Int = 1`
  *       : information about neighbours nodes set to X number of hops away from original vertex. Default is set to 1 hop away.
  *
  *   ## States
  *
  *   {s}`vertexInvolved: Boolean`
  *     : Boolean flag of whether vertex should be included in the final tabularised output.
  *
  *  ## Returns
  *
  *   {
  *      "label": "Goldilocks",
  *      "metadata": {
  *        "id": "5415127257870295999"
  *      },
  *      "edges": [
  *        {
  *          "source": "5415127257870295999",
  *          "target": "2751396522352401052",
  *          "metadata": {
  *            "value": 0
  *          }
  *        },
  *        {
  *          "source": "5415127257870295999",
  *          "target": "-6281184428558944342",
  *          "metadata": {
  *            "value": 0
  *          }
  *        }
  *      ]
  *    }
  *
  *   ```{note}
  *   To be used with JsonFormat.scala and JsonOutputRunner.scala (in lotrTopic example) to return this Json format.
  *   ```
  */

class NodeInformation(initialID: Long, hopsAway: Int = 1) extends Generic {

  case class Node(label: String, metadata: NodeData, edges: Array[EdgeInfo])
  case class NodeData(id: String)
  case class EdgeInfo(source: String, target: String, metadata: EdgeData)
  case class EdgeData(value: Int)

  override def apply(graph: GraphPerspective): graph.Graph =
    graph
      .step { vertex =>
        if (vertex.ID == initialID) {
          vertex.setState("id", vertex.ID)
          vertex.setState("name", vertex.name())
          vertex.setState("vertexInvolved", true)
          val edgeInformation: Array[EdgeInfo] = vertex.edges.map { edge =>
            EdgeInfo(
                    edge.src.toString,
                    edge.dst.toString,
                    EdgeData(edge.weight(weightProperty = "Character Co-occurence", 0))
            )
          }.toArray
          vertex.setState(
                  "NodeInformation",
                  Node(
                          name,
                          NodeData(vertex.ID.toString),
                          edgeInformation
                  )
          )
          vertex.messageAllNeighbours(true)
        }
      }
      .iterate(
              { vertex =>
                val edgeInformation: Array[EdgeInfo] = vertex.edges.map { edge =>
                  EdgeInfo(
                          edge.src.toString,
                          edge.dst.toString,
                          EdgeData(edge.weight(weightProperty = "Character Co-occurence", 0))
                  )
                }.toArray
                vertex.setState("vertexInvolved", true)
                vertex.setState(
                        "NodeInformation",
                        Node(
                                name,
                                NodeData(vertex.ID.toString),
                                edgeInformation
                        )
                )
                if (hopsAway > 1)
                  vertex.messageAllNeighbours(true)
              },
              hopsAway,
              true
      )

  override def tabularise(graph: GraphPerspective): Table =
    graph
      .select("NodeInformation")
      .filter(row => row.values().nonEmpty)
}

object NodeInformation {
  def apply(initialID: Long, hopsAway: Int = 1) = new NodeInformation(initialID, hopsAway)
}
