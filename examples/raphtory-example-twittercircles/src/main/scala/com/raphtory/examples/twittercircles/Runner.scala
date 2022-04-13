package com.raphtory.examples.twittercircles

import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.deployment.Raphtory
import com.raphtory.output.FileOutputFormat
import com.raphtory.output.PulsarOutputFormat
import com.raphtory.spouts.StaticGraphSpout
import com.raphtory.examples.twittercircles.graphbuilders.TwitterCirclesGraphBuilder
import com.raphtory.util.FileUtils

object Runner extends App {
  // Create Graph
  val path = "/tmp/snap-twitter.csv"
  val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/snap-twitter.csv"
  FileUtils.curlFile(path, url)

  val source  = StaticGraphSpout(path)
  val builder = new TwitterCirclesGraphBuilder()
  val graph   = Raphtory.streamGraph(spout = source, graphBuilder = builder)
  Thread.sleep(20000)
  graph.pointQuery(EdgeList(), PulsarOutputFormat("TwitterEdgeList"), 88234)
  graph.rangeQuery(
          ConnectedComponents(),
          PulsarOutputFormat("ConnectedComponents"),
          10000,
          88234,
          10000,
          List(500, 1000, 10000)
  )
}
