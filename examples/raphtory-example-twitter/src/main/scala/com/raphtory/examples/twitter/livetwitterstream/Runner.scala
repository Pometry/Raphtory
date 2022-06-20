package com.raphtory.examples.twitter.livetwitterstream

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.sinks.PulsarSink
import com.raphtory.twitter.builder.LiveTwitterRetweetGraphBuilder
import com.raphtory.twitter.builder.LiveTwitterUserGraphBuilder
import com.raphtory.twitter.spout.LiveTwitterSpout
import com.typesafe.config.Config

object Runner extends IOApp {
  val raphtoryConfig: Config = Raphtory.getDefaultConfig()

  val enableRetweetGraphBuilder: Boolean =
    raphtoryConfig.getBoolean("raphtory.spout.twitter.local.enableRetweetFilter")

  override def run(args: List[String]): IO[ExitCode] = {

    val source  = new LiveTwitterSpout()
    val output  = PulsarSink("EdgeList")
    val builder =
      if (enableRetweetGraphBuilder)
        new LiveTwitterRetweetGraphBuilder()
      else
        new LiveTwitterUserGraphBuilder()
    Raphtory.stream(spout = source, graphBuilder = builder).use { graph =>
      IO {
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
