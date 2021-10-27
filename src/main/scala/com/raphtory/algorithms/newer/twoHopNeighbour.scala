package com.raphtory.algorithms.newer

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}

import scala.collection.mutable

// TODO ADD ARG SO CAN CHECK ONE NODE AT A TIME RATHER THAN WHOLE GRAPH
class twoHopNeighbour(output: String = "/tmp/twoHopNeighbour") extends GraphAlgorithm {
  override def algorithm(graph: GraphPerspective): Unit = {
    graph
      .step(vertex => vertex.getEdges().foreach(edge => edge.send(("twoHopRequest", vertex.ID, 0))))
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
        }, 5
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
  def apply(output: String) = new twoHopNeighbour(output)
}
