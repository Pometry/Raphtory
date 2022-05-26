package com.raphtory.examples.lotrTopic

import com.raphtory.algorithms.api.Alignment
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
  val graph   = Raphtory.batchLoad(spout = source, graphBuilder = builder)

  // Run algorithms
  graph
    .at(30000)
    .past()
    .execute(EdgeList())
    .writeTo(PulsarOutputFormat("EdgeList"))
  graph
    .range(20000, 30000, 10000)
    .window(List(500, 1000, 10000), Alignment.END)
    .execute(PageRank())
    .writeTo(PulsarOutputFormat("PageRank"))
}
