package com.raphtory.algorithms.newer

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}

import scala.collection.mutable

/*
  Description: Two hop neighbours.
  given a start node, or not given the start_node, this will
  go through all nodes in the graph and find their two-hop neighbours.
  The algorithm works as follows,
  in the first step the node messages all its neighbours, saying that it is
  asking for a two-hop analysis.
  When the node receives this request, it then finds all of its neighbours
  and replies to the node in the form (response, neighbour, me).
  Warning: As this sends alot of messages between nodes, running this for the
  entire graph with a large number of iterations may cause you to run out of memory.
  Therefore it is most optimal to run with a select node at a time.
  The number of iterations makes a difference to ensure all messages have been read.
  Output: The result is then aggregated and output into a csv file which has the following
  output:   (time the algorithm was run, start node, first hop, second hop).
 */

class twoHopNeighbour(nodeID:Long = -1, output: String = "/tmp/twoHopNeighbour") extends GraphAlgorithm {
  override def algorithm(graph: GraphPerspective): Unit = {
    graph
      .step(
        vertex =>
          if (nodeID == -1){
            vertex.getEdges().foreach(edge => edge.send(("twoHopRequest", vertex.ID, 0)))
          }
          else if (vertex.ID() == nodeID){
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
        }, 100
      )
      .select(vertex =>
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
          row.get(2).asInstanceOf[mutable.ListBuffer[(Long, Long)]]
            .toList.map(hops =>
              Row(row.get(1), hops._1, hops._2)
          )
      )
      .writeTo(output)
  }
}

object twoHopNeighbour {
  def apply(nodeID: Long= -1, output: String= "/tmp/twoHopNeighbour") = new twoHopNeighbour(nodeID, output)
}
