package com.raphtory.algorithms

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row, Table}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
Description
  This algorithm will return the two hop neighbours of each node in
  the graph. If the user provides a node ID, then it will only return
  the two hop neighbours of that node.
  1. In the first step the node messages all its neighbours, saying that it is
  asking for a two-hop analysis.
  2. Each vertex, starting from a triangle count of zero, looks at the lists
  of ID requests it has received, it then finds all of its neighbours and
  replies to the node in the form (response, neighbour, me).
  3. The requester compiles these into a list of results

Parameters
  node (String) : The node ID to start with. If not specified, then this is
                  run for all nodes.
  output (String) : The path where the output will be saved. If not specified,
                  defaults to /tmp/twoHopNeighbour

Returns
  ID (Long) : Vertex ID
  Triangle Count (Long) : Number of triangles

Warning
  As this sends alot of messages between nodes, running this for the entire
  graph with a large number of iterations may cause you to run out of memory.
  Therefore it is most optimal to run with a select node at a time.
  The number of iterations makes a difference to ensure all messages have been read.
**/
class twoHopNeighbour(nodeID:Long = -1, output: String = "/tmp/twoHopNeighbour") extends GraphAlgorithm {
  override def graphStage(graph: GraphPerspective): GraphPerspective = {
    graph
      .step(
        vertex =>
          if (nodeID == -1 || vertex.ID() == nodeID) {
            vertex.getEdges().foreach(edge => edge.send(("twoHopRequest", vertex.ID, 0)))
          }
      )
      .iterate(
        { vertex =>
          val newMessages = vertex.messageQueue[(String, Long, Long)]
          val requests = newMessages.distinct.filter(_._1 == "twoHopRequest")
          val responses = newMessages.distinct.filter(_._1 == "twoHopResponse")
          if (requests.nonEmpty) {
            vertex.getAllNeighbours().foreach { neighbour =>
              requests.foreach(msg =>
                if (msg._2 != neighbour) {
                  vertex.messageNeighbour(msg._2, ("twoHopResponse", neighbour, vertex.ID))
                })
            }
          }
          if (responses.nonEmpty) {
            vertex.setState("twoHopResponse", true)
            var twoHops = vertex.getOrSetState("twoHops", mutable.ListBuffer[(Long, Long)]())
            responses.foreach(response => twoHops.append((response._2, response._3)))
            twoHops = twoHops.distinct
            vertex.setState("twoHops", twoHops)
          }
        }, 25, true
      )
  }

  override def tableStage(graph: GraphPerspective): Table = {
    graph.select(vertex =>
      Row(
        vertex.getStateOrElse("twoHopResponse", false),
        vertex.ID(),
        vertex.getStateOrElse("twoHops", "")
      )
    )
      .filter(
        row =>
          row.get(0) == true)
      .explode(
        row =>
          row.get(2).asInstanceOf[ListBuffer[(Long, Long)]]
            .toList.map(hops =>
            Row(row.get(1), hops._1, hops._2)
          )
      )
  }

  override def write(table: Table): Unit = {
    table.writeTo(output)
  }
}

object twoHopNeighbour {
  def apply(nodeID: Long= -1, output: String= "/tmp/twoHopNeighbour") = new twoHopNeighbour(nodeID, output)
}
