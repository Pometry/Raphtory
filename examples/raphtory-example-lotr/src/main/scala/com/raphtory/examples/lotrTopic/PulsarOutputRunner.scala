package com.raphtory.examples.lotrTopic

import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.algorithms.generic.centrality.PageRank
import com.raphtory.deployment.Raphtory
import com.raphtory.output.PulsarOutputFormat
import com.raphtory.examples.lotrTopic.graphbuilders.LOTRGraphBuilder
import com.raphtory.spouts.FileSpout
import com.raphtory.spouts.ResourceSpout
import com.raphtory.util.FileUtils

object PulsarOutputRunner extends App {

  val path = "/tmp/lotr.csv"
  val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"

  FileUtils.curlFile(path, url)

  // Create Graph
  val source  = FileSpout(path)
  val builder = new LOTRGraphBuilder()
  val graph   = Raphtory.streamGraph(spout = source, graphBuilder = builder)
  Thread.sleep(20000)

  // Run algorithms
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
