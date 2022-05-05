package com.raphtory.examples.enron

import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.deployment.Raphtory
import com.raphtory.output.FileOutputFormat
import com.raphtory.output.PulsarOutputFormat
import com.raphtory.examples.enron.graphbuilders.EnronGraphBuilder
import com.raphtory.spouts.FileSpout
import com.raphtory.util.FileUtils

object Runner extends App {

  val path = "/tmp/email_test.csv"
  val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/email_test.csv"
  FileUtils.curlFile(path, url)

  // Create Graph
  val source  = FileSpout(path)
  val builder = new EnronGraphBuilder()
  val graph   = Raphtory.stream(spout = source, graphBuilder = builder)
  Thread.sleep(20000)

  graph
    .at(989858340000L)
    .past()
    .execute(EdgeList())
    .writeTo(PulsarOutputFormat("EdgeList"))
  graph
    .range(963557940000L, 989858340000L, 1000000000)
    .past()
    .execute(ConnectedComponents())
    .writeTo(PulsarOutputFormat("ConnectedComponents"))
}
