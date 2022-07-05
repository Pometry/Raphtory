package com.raphtory.examples.twitter.followernetwork

import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.{ConnectedComponents, EdgeList}
import com.raphtory.api.analysis.graphview.Alignment
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

  val source  = StaticGraphSpout(path)
  val builder = new TwitterCirclesGraphBuilder()
  Using(Raphtory.stream(spout = source, graphBuilder = builder)) { graph =>
    graph
      .at(88234)
      .past()
      .execute(EdgeList())
      .writeTo(PulsarSink("TwitterEdgeList"))
      .waitForJob()

    graph
      .range(10000, 88234, 10000)
      .window(List(500, 1000, 10000), Alignment.END)
      .execute(ConnectedComponents())
      .writeTo(PulsarSink("ConnectedComponents"))
      .waitForJob()
  }

}
