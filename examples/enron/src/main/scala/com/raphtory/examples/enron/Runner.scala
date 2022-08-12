package com.raphtory.examples.enron

import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.api.input.Source
import com.raphtory.examples.enron.graphbuilders.EnronGraphBuilder
import com.raphtory.sinks.PulsarSink
import com.raphtory.spouts.FileSpout
import com.raphtory.utils.FileUtils

object Runner extends App {

  val path = "/tmp/email_test.csv"
  val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/email_test.csv"
  FileUtils.curlFile(path, url)

  // Create Graph
  val spout   = FileSpout(path)
  val builder = new EnronGraphBuilder()
  val source  = Source(spout, builder)
  val graph   = Raphtory.newGraph()
  graph.ingest(source)
  try {
    graph
      .at(989858340000L)
      .past()
      .execute(EdgeList())
      .writeTo(PulsarSink("EdgeList"))
      .waitForJob()

    graph
      .range(963557940000L, 989858340000L, 1000000000)
      .past()
      .execute(ConnectedComponents())
      .writeTo(PulsarSink("ConnectedComponents"))
      .waitForJob()
  }
  finally graph.close()
}
