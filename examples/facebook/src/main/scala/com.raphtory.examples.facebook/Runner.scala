package com.raphtory.examples.facebook

import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.api.analysis.graphview.Alignment
import com.raphtory.api.input.Source
import com.raphtory.examples.facebook.graphbuilders.FacebookGraphBuilder
import com.raphtory.sinks.PulsarSink
import com.raphtory.spouts.StaticGraphSpout

import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import scala.language.postfixOps
import scala.sys.process._
import scala.util.Using

object Runner extends App {

  if (!new File("/tmp", "facebook.csv").exists()) {
    val path = "/tmp/facebook.csv"
    try s"curl -o $path https://raw.githubusercontent.com/Raphtory/Data/main/facebook.csv " !!
    catch {
      case ex: Exception =>
        Files.deleteIfExists(Paths.get(path))
        throw ex
    }
  }

  val spout: StaticGraphSpout = StaticGraphSpout("/tmp/facebook.csv")
  val builder                 = new FacebookGraphBuilder()
  val source                  = Source(spout, builder)
  val graph                   = Raphtory.newGraph()
  graph.ingest(source)

  Using(graph) { graph =>
    graph
      .at(88234)
      .past()
      .execute(EdgeList())
      .writeTo(PulsarSink("EdgeList"))

    graph
      .range(10000, 88234, 10000)
      .window(List(500, 1000, 10000), Alignment.END)
      .execute(ConnectedComponents)
      .writeTo(PulsarSink("ConnectedComponents"))
  }
}
