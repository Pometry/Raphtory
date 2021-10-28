package com.raphtory.dev.lotr
import com.raphtory.core.build.client.RaphtoryClient
import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}
object LOTRClient extends App {
  val client = new RaphtoryClient("127.0.0.1:1600",1700)

  client.pointQuery(dd,10000,List(10000, 1000,100))
}


object dd extends GraphAlgorithm {
  override def algorithm(graph: GraphPerspective): Unit = {
    graph
      .step({
        vertex =>
          vertex.setState("cclabel", vertex.ID)
          vertex.messageAllNeighbours(vertex.ID)
      })
      .iterate({
        vertex =>
          val label = vertex.messageQueue[Long].min
          if (label < vertex.getState[Long]("cclabel")) {
            vertex.setState("cclabel", label)
            vertex messageAllNeighbours label
          }
          else
            vertex.voteToHalt()
      }, 100)
      .select(vertex => Row(vertex.ID(),vertex.getState[Long]("cclabel")))
      .writeTo("/Users/bensteer/github/output")

  }
}

