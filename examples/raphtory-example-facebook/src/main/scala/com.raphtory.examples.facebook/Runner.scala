package com.raphtory.examples.facebook

import com.raphtory.algorithms.api.Alignment
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.deployment.Raphtory
import com.raphtory.output.FileOutputFormat
import com.raphtory.output.PulsarOutputFormat
import com.raphtory.examples.facebook.graphbuilders.FacebookGraphBuilder
import com.raphtory.spouts.StaticGraphSpout
import com.typesafe.config

import java.io.File
import scala.language.postfixOps
import sys.process._

object Runner extends App {

  // Create Graph

  if (!new File("/tmp", "facebook.csv").exists()) {
    val path = "/tmp/facebook.csv"
    try s"curl -o $path https://raw.githubusercontent.com/Raphtory/Data/main/facebook.csv " !!
    catch {
      case ex: Exception =>
        ex.printStackTrace()

        (s"rm $path" !)
        throw ex
    }
  }

  val source: StaticGraphSpout = StaticGraphSpout("/tmp/facebook.csv")
  val builder                  = new FacebookGraphBuilder()
  val graph                    = Raphtory.load(spout = source, graphBuilder = builder)
  Thread.sleep(20000)
  graph.at(88234).past().execute(EdgeList()).writeTo(PulsarOutputFormat("EdgeList"))
  graph
    .range(10000, 88234, 10000)
    .window(List(500, 1000, 10000), Alignment.END)
    .execute(ConnectedComponents())
    .writeTo(PulsarOutputFormat("ConnectedComponents"))
}
