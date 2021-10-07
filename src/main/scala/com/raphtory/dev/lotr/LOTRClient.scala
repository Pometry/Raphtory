package com.raphtory.dev.lotr
import com.raphtory.core.build.client.RaphtoryClient
import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}
import com.raphtory.dev.lotr.TestAlgorithm
object LOTRClient extends App {
  val client = new RaphtoryClient("127.0.0.1:1601",1700)
  val algo = new GraphAlgorithm {
    override def algorithm(graph: GraphPerspective): Unit = {
      graph.step({
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
        .select(vertex => Row(Array(vertex.ID(),vertex.getState[Long]("cclabel"))))
        //.filter(r=> r.get(0).asInstanceOf[Long]==18174)
        .writeTo("/Users/bensteer/github/output")
    }
  }
  client.pointQuery(algo,10000,List(10000, 1000,100))
}


