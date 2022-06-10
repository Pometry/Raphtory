package com.raphtory.examples.gab;

import com.raphtory.Raphtory
import com.raphtory.examples.gab.graphbuilders.GabUserGraphBuilder
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.api.analysis.graphview.Alignment
import com.raphtory.api.input.Spout
import com.raphtory.sinks.PrintSink
import com.raphtory.sinks.PulsarSink
import com.raphtory.spouts.FileSpout
import com.raphtory.spouts.ResourceSpout
import com.raphtory.utils.FileUtils

object Runner extends App {

  val path                  = "/tmp/gabNetwork500.csv"
  val url                   = "https://raw.githubusercontent.com/Raphtory/Data/main/gabNetwork500.csv"
  FileUtils.curlFile(path, url)
  val source: Spout[String] = FileSpout(path)
  val builder               = new GabUserGraphBuilder()
  val rg                    = Raphtory.stream(spout = source, graphBuilder = builder)
  val sink                  = PulsarSink("Gab")
  rg.at(1476113856000L)
    .past()
    .execute(EdgeList())
    .writeTo(PulsarSink("EdgeList"))
  rg.range(1470797917000L, 1476113856000L, 86400000L)
    .window(List(3600000L, 86400000L, 604800000L, 2592000000L, 31536000000L), Alignment.END)
    .execute(ConnectedComponents)
    .writeTo(sink)

}
