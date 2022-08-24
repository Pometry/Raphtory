package com.raphtory.examples.twitter.followernetwork

import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.api.analysis.graphview.Alignment
import com.raphtory.api.input.Source
import com.raphtory.examples.twitter.followernetwork.graphbuilders.TwitterCirclesGraphBuilder
import com.raphtory.sinks.PulsarSink
import com.raphtory.spouts.StaticGraphSpout
import com.raphtory.utils.FileUtils

import scala.util.Using

object Runner extends App {

  // Create Graph
  val path = "/tmp/snap-twitter.csv"
  val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/snap-twitter.csv"
  FileUtils.curlFile(path, url)

  val spout   = StaticGraphSpout(path)
  val builder = new TwitterCirclesGraphBuilder()
  val source  = Source(spout, builder)
  val graph   = Raphtory.newGraph()
  graph.blockingIngest(source)
  Using(graph) { graph =>
    graph
      .at(88234)
      .past()
      .execute(EdgeList())
      .writeTo(PulsarSink("TwitterEdgeList"))
      .waitForJob()

    graph
      .range(10000, 88234, 10000)
      .window(List(500, 1000, 10000), Alignment.END)
      .execute(ConnectedComponents)
      .writeTo(PulsarSink("ConnectedComponents"))
      .waitForJob()
  }

}
