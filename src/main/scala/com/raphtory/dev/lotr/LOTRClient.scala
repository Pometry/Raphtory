package com.raphtory.dev.lotr
import com.raphtory.algorithms.newer.ConnectedComponents
import com.raphtory.core.build.client.RaphtoryClient
import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}
object LOTRClient extends App {
  val client = new RaphtoryClient("127.0.0.1:1601",1700)
  client.pointQuery(ConnectedComponents("/Users/bensteer/github/output"),10000,List(10000, 1000,100))
}


