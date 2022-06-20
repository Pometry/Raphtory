package com.raphtory.twitter

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
import io.github.redouane59.twitter.dto.tweet.Tweet

/**
  * To utilise this test, you must add your Twitter API credentials in application.conf under Raphtory.spout.twitter.local
  * If you would like to filter a hashtag, you can add this under Raphtory.spout.twitter.local.hashtag in application.conf
  */
object LiveTwitterTest extends IOApp {

  override def run(args: List[String]): IO[ExitCode] = {

    val raphtoryConfig: Config = Raphtory.getDefaultConfig()

    val enableRetweetGraphBuilder: Boolean =
      raphtoryConfig.getBoolean("raphtory.spout.twitter.local.enableRetweetFilter")

    val spout        = new LiveTwitterSpout()
    val graphBuilder =
      if (enableRetweetGraphBuilder)
        new LiveTwitterRetweetGraphBuilder()
      else
        new LiveTwitterUserGraphBuilder()

    Raphtory.stream[Tweet](spout, graphBuilder).use { graph =>
      IO {
        graph
          .walk("5 milliseconds")
          .window("5 milliseconds")
          .execute(EdgeList())
          .writeTo(PulsarSink("EdgeList1"))
          .waitForJob()

        ExitCode.Success
      }
    }
  }

}
