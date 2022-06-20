package com.raphtory.examples.twitter.followernetwork

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.api.analysis.graphview.Alignment
import com.raphtory.examples.twitter.followernetwork.graphbuilders.TwitterCirclesGraphBuilder
import com.raphtory.sinks.PulsarSink
import com.raphtory.spouts.StaticGraphSpout
import com.raphtory.utils.FileUtils

object Runner extends IOApp {

  override def run(args: List[String]): IO[ExitCode] = {

    // Create Graph
    val path = "/tmp/snap-twitter.csv"
    val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/snap-twitter.csv"
    FileUtils.curlFile(path, url)

    val source  = StaticGraphSpout(path)
    val builder = new TwitterCirclesGraphBuilder()
    Raphtory.stream(spout = source, graphBuilder = builder).use { graph =>
      IO {
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
        ExitCode.Success
      }
    }
  }
}
