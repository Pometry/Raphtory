package com.raphtory.algorithms.newer

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective}
import scala.collection.mutable

class twoHopNeighbour(output:String = "/tmp/twoHopNeighbour") extends GraphAlgorithm{
  override def algorithm(graph: GraphPerspective): Unit = {
    graph.step({
      vertex => vertex.getEdges().foreach(edge => edge.send(
          ("twoHopRequest", vertex.ID, 0)
        )
        )
    }).iterate({
      vertex =>
        val newMessages = vertex.messageQueue[(String, Long, Int)]
        val request = newMessages.filter(item => item.get(0)=="twoHopRequest")
        newMessages.filter(item => item.get(0)!="twoHopRequest").foreach(msg => vertex.messageSelf(msg))
        request.foreach(
          msg =>
            vertex.messageNeighbour(
              msg.get(1).get(0),
            )
        )

        vertex.getAllNeighbours().foreach(
          neighbour =>
            request.map({
              m =>
                if (m.size == 1) {
                  neighbour =>
                  )
                }
        )
        val twoHopStatus =

        })
        vertex.getAllNeighbours()
    }, 50)
  }
}


object twoHopNeighbour{
  def apply(output:String) = new twoHopNeighbour(output)
}

