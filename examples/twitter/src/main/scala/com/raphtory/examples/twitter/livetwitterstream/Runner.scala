package com.raphtory.examples.twitter.livetwitterstream

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.api.input.Source
import com.raphtory.sinks.PulsarSink
import com.raphtory.twitter.builder.TwitterRetweetGraphBuilder
import com.raphtory.twitter.builder.TwitterUserGraphBuilder
import com.raphtory.twitter.spout.LiveTwitterSpout
import com.typesafe.config.Config
import io.github.redouane59.twitter.dto.tweet.Tweet

object Runner extends IOApp {
  val raphtoryConfig: Config = Raphtory.getDefaultConfig()

  val enableRetweetGraphBuilder: Boolean =
    raphtoryConfig.getBoolean("raphtory.spout.twitter.local.enableRetweetFilter")

  override def run(args: List[String]): IO[ExitCode] = {

    val spout   = LiveTwitterSpout()
    val output  = PulsarSink("EdgeList")
    val builder =
      if (enableRetweetGraphBuilder)
        TwitterRetweetGraphBuilder
      else
        TwitterUserGraphBuilder

    val source = Source(spout, builder)
    val graph  = Raphtory.newIOGraph()

    graph.use { graph =>
      IO {
        graph.load(source)
        graph
          .walk("10 milliseconds")
          .window("10 milliseconds")
          .execute(EdgeList())
          .writeTo(output)
          .waitForJob()
        ExitCode.Success
      }
    }
  }
}
