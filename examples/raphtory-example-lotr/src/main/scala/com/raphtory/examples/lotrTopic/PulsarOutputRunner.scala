package com.raphtory.examples.lotrTopic

import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.algorithms.generic.centrality.PageRank
import com.raphtory.deployment.Raphtory
import com.raphtory.output.PulsarOutputFormat
import com.raphtory.examples.lotrTopic.graphbuilders.LOTRGraphBuilder
import com.raphtory.spouts.{FileSpout, ResourceSpout}

object PulsarOutputRunner extends App {

  // Create Graph
  val source  = FileSpout("/Users/haaroony/Documents/Raphtory/examples/raphtory-example-lotr/lotr.csv")
  val builder = new LOTRGraphBuilder()
  val graph   = Raphtory.streamGraph(spout = source, graphBuilder = builder)
  Thread.sleep(20000)

  // Run algorihtms
  graph.pointQuery(EdgeList(), PulsarOutputFormat("EdgeList"), timestamp = 30000)
  graph.rangeQuery(
          PageRank(),
          PulsarOutputFormat("PageRank"),
          20000,
          30000,
          10000,
          List(500, 1000, 10000)
  )
}
