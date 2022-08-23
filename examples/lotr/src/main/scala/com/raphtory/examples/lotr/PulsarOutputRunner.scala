package com.raphtory.examples.lotr

import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.algorithms.generic.centrality.PageRank
import com.raphtory.api.analysis.graphview.Alignment
import com.raphtory.api.analysis.graphview.DeployedTemporalGraph
import com.raphtory.api.analysis.graphview.TemporalGraph
import com.raphtory.examples.lotr.graphbuilders.LOTRGraphBuilder
import com.raphtory.sinks.PulsarSink
import com.raphtory.spouts.FileSpout
import com.raphtory.utils.FileUtils

import scala.util.Using

object PulsarOutputRunner extends App {

  val path = "/tmp/lotr.csv"
  val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"

  FileUtils.curlFile(path, url)

  // Create Graph
  val source                       = FileSpout(path)
  val builder                      = new LOTRGraphBuilder()
  val graph: DeployedTemporalGraph = Raphtory.newGraph()
  graph.ingest(com.raphtory.api.input.Source(FileSpout(path.toString), builder))
  Using(graph) { graph =>
    graph
      .at(30000)
      .past()
      .execute(EdgeList())
      .writeTo(PulsarSink("EdgeList"))
      .waitForJob()

    graph
      .range(20000, 30000, 10000)
      .window(List(500, 1000, 10000), Alignment.END)
      .execute(PageRank())
      .writeTo(PulsarSink("PageRank"))
      .waitForJob()
  }
}
