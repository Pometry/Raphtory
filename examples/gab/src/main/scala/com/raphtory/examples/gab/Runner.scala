package com.raphtory.examples.gab;

import com.raphtory.RaphtoryApp
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.api.analysis.graphview.Alignment
import com.raphtory.api.input.Source
import com.raphtory.examples.gab.graphbuilders.GabUserGraphBuilder
import com.raphtory.internals.context.RaphtoryContext
import com.raphtory.sinks.FileSink
import com.raphtory.spouts.FileSpout
import com.raphtory.utils.FileUtils

object Runner extends RaphtoryApp.Remote("localhost", 1736) {
//object Runner extends RaphtoryApp.Local {

  val path = "/tmp/gabNetwork500.csv"
  val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/gabNetwork500.csv"
  FileUtils.curlFile(path, url)

  override def run(args: Array[String], ctx: RaphtoryContext): Unit =
    ctx.runWithNewGraph() { graph =>
      val source = Source(FileSpout(path), GabUserGraphBuilder)
      graph.addDynamicPath("com.raphtory.examples")
      graph.load(source)

      graph
        .at(1476113856000L)
        .past()
        .execute(EdgeList())
        .writeTo(FileSink("/tmp/raphtory/Gab"))
        .waitForJob()

      graph
        .range(1470797917000L, 1476113856000L, 86400000L)
        .window(Array(3600000L, 86400000L, 604800000L, 2592000000L, 31536000000L), Alignment.END)
        .execute(ConnectedComponents)
        .writeTo(FileSink("/tmp/raphtory/Gab"))
        .waitForJob()
    }
}
