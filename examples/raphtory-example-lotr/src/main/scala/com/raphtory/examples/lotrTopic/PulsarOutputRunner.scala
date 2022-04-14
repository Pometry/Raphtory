package com.raphtory.examples.lotrTopic

import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.algorithms.generic.centrality.PageRank
import com.raphtory.deployment.Raphtory
import com.raphtory.output.{FileOutputFormat, PulsarOutputFormat}
import com.raphtory.examples.lotrTopic.graphbuilders.LOTRGraphBuilder
import com.raphtory.spouts.{FileSpout, ResourceSpout}

object PulsarOutputRunner extends App {

  // Create Graph
  val source  = FileSpout("/Users/haaroony/Documents/Raphtory/examples/raphtory-example-lotr/lotr.csv")
  val builder = new LOTRGraphBuilder()
  val graph   = Raphtory.streamGraph(spout = source, graphBuilder = builder)
  val trackerA = graph.pointQuery(EdgeList(), FileOutputFormat("/tmp/22EdgeList"), timestamp = 30000)
//  Thread.sleep(20000)
//  println("yo")
  // Run algorihtms
  val client = Raphtory.createClient()
  val trackerB = client.pointQuery(EdgeList(), FileOutputFormat("/tmp/22EdgeList"), timestamp = 30000)
  val x = 0

  // graph.pointQuery(EdgeList(), FileOutputFormat("/tmp/2EdgeList"), timestamp = 30000)
//  graph.rangeQuery(
//          PageRank(),
//          PulsarOutputFormat("PageRank"),
//          20000,
//          30000,
//          10000,
//          List(500, 1000, 10000)
//  )
}
