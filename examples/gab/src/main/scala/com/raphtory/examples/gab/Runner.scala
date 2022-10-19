package com.raphtory.examples.gab;

import com.raphtory.RaphtoryApp
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.api.analysis.graphview.Alignment
import com.raphtory.api.input.Source
import com.raphtory.examples.gab.graphbuilders.GabUserGraphBuilder
import com.raphtory.internals.context.RaphtoryContext
import com.raphtory.pulsar.sink.PulsarSink
import com.raphtory.spouts.FileSpout
import com.raphtory.utils.FileUtils

object Runner extends RaphtoryApp {

  val path = "/tmp/gabNetwork500.csv"
  val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/gabNetwork500.csv"
  FileUtils.curlFile(path, url)

  override def buildContext(): RaphtoryContextType = LocalContext()

  override def run(args: Array[String], ctx: RaphtoryContext): Unit =
    ctx.runWithNewGraph() { graph =>
      val source = Source(FileSpout(path), GabUserGraphBuilder)

      graph.load(source)

      graph
        .at(1476113856000L)
        .past()
        .execute(EdgeList())
        .writeTo(PulsarSink("EdgeList"))
        .waitForJob()

      graph
        .range(1470797917000L, 1476113856000L, 86400000L)
        .window(List(3600000L, 86400000L, 604800000L, 2592000000L, 31536000000L), Alignment.END)
        .execute(ConnectedComponents)
        .writeTo(PulsarSink("Gab"))
        .waitForJob()
    }
}
