package com.raphtory.examples.lotr

import com.raphtory.RaphtoryApp
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.algorithms.generic.centrality.PageRank
import com.raphtory.api.analysis.graphview.Alignment
import com.raphtory.internals.context.RaphtoryContext
import com.raphtory.pulsar.sink.PulsarSink
import com.raphtory.sources.CSVEdgeListSource
import com.raphtory.utils.FileUtils

object PulsarOutputRunner extends RaphtoryApp.Local {

  override def run(args: Array[String], ctx: RaphtoryContext): Unit = {
    val path = "/tmp/lotr.csv"
    val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"

    FileUtils.curlFile(path, url)

    ctx.runWithNewGraph() { graph =>
      graph.load(CSVEdgeListSource.fromFile(path))

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
}
