package com.raphtory.examples.twitter.followernetwork

import com.raphtory.algorithms.api.Alignment
import com.raphtory.algorithms.generic.{ConnectedComponents, EdgeList}
import com.raphtory.deployment.Raphtory
import com.raphtory.examples.twitter.followernetwork.graphbuilders.TwitterCirclesGraphBuilder
import com.raphtory.output.PulsarOutputFormat
import com.raphtory.spouts.StaticGraphSpout
import com.raphtory.util.FileUtils

object Runner extends App {
  // Create Graph
  val path = "/tmp/snap-twitter.csv"
  val url = "https://raw.githubusercontent.com/Raphtory/Data/main/snap-twitter.csv"
  FileUtils.curlFile(path, url)

  val source = StaticGraphSpout(path)
  val builder = new TwitterCirclesGraphBuilder()
  val graph = Raphtory.stream(spout = source, graphBuilder = builder)
  Thread.sleep(20000)
  graph
    .at(88234)
    .past()
    .execute(EdgeList())
    .writeTo(PulsarOutputFormat("TwitterEdgeList"))
  graph
    .range(10000, 88234, 10000)
    .window(List(500, 1000, 10000), Alignment.END)
    .execute(ConnectedComponents())
    .writeTo(PulsarOutputFormat("ConnectedComponents"))
}
