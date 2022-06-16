package com.raphtory.examples.lotrTopic

import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.algorithms.generic.centrality.PageRank
import com.raphtory.api.analysis.graphview.Alignment
import com.raphtory.examples.lotrTopic.graphbuilders.LOTRGraphBuilder
import com.raphtory.sinks.PulsarSink
import com.raphtory.spouts.FileSpout
import com.raphtory.utils.FileUtils

object PulsarOutputRunner extends App {

  val path = "/tmp/lotr.csv"
  val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"

  FileUtils.curlFile(path, url)

  // Create Graph
  val source  = FileSpout(path)
  val builder = new LOTRGraphBuilder()
  val graph   = Raphtory.load(spout = source, graphBuilder = builder)

  // Run algorithms
  graph
    .at(30000)
    .past()
    .execute(EdgeList())
    .writeTo(PulsarSink("EdgeList"))
  graph
    .range(20000, 30000, 10000)
    .window(List(500, 1000, 10000), Alignment.END)
    .execute(PageRank())
    .writeTo(PulsarSink("PageRank"))
}
