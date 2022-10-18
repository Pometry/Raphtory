package com.raphtory.examples.lotr

import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.algorithms.generic.centrality.PageRank
import com.raphtory.api.analysis.graphview.Alignment
import com.raphtory.api.analysis.graphview.DeployedTemporalGraph
import com.raphtory.api.analysis.graphview.TemporalGraph
import com.raphtory.api.input.sources.CSVEdgeListSource
import com.raphtory.pulsar.sink.PulsarSink
import com.raphtory.spouts.FileSpout
import com.raphtory.utils.FileUtils

object PulsarOutputRunner extends App {

  val path = "/tmp/lotr.csv"
  val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"

  FileUtils.curlFile(path, url)

  // Create Graph
  val source                       = FileSpout(path)
  val graph: DeployedTemporalGraph = Raphtory.newGraph()
  graph.load(CSVEdgeListSource.fromFile(path))
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
