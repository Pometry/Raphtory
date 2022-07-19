package com.raphtory.examples.lotr

import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.algorithms.generic.centrality.PageRank
import com.raphtory.examples.lotr.graphbuilders.LOTRGraphBuilder
import com.raphtory.sinks.FileSink
import com.raphtory.spouts.FileSpout
import com.raphtory.utils.FileUtils

import scala.language.postfixOps

object TutorialRunner extends App {

  val path = "/tmp/lotr.csv"
  val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"

  FileUtils.curlFile(path, url)

  val source  = FileSpout(path)
  val builder = new LOTRGraphBuilder()
  val graph   = Raphtory.load(spout = source, graphBuilder = builder)

  val output = FileSink("/tmp/raphtory_scala_cc")

  graph
    .at(32674)
    .past()
    .execute(ConnectedComponents())
    .writeTo(output)
    .waitForJob()

  graph.close()
}
