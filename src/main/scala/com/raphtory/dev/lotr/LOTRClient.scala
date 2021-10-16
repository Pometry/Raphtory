package com.raphtory.dev.lotr
import com.raphtory.algorithms.newer.ConnectedComponents
import com.raphtory.core.build.client.RaphtoryClient
import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}
object LOTRClient extends App {
  val client = new RaphtoryClient("127.0.0.1:1601",1700)

  client.pointQuery(new testAlgo,10000,List(10000, 1000,100))
}


class testAlgo()extends GraphAlgorithm {
  override def algorithm(graph: GraphPerspective): Unit = {
  val count = graph.nodeCount()
  graph.step(v=> v.setState("key",count))
  .select(v => Row(v.ID(),v.getState("key")))
  .writeTo("/Users/bensteer/github/output")
}
}

