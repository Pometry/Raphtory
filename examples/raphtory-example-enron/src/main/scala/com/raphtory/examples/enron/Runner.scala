package com.raphtory.examples.enron

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.examples.enron.graphbuilders.EnronGraphBuilder
import com.raphtory.sinks.FileSink
import com.raphtory.sinks.PulsarSink
import com.raphtory.spouts.FileSpout
import com.raphtory.utils.FileUtils

object Runner extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {

    val path = "/tmp/email_test.csv"
    val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/email_test.csv"
    FileUtils.curlFile(path, url)

    // Create Graph
    val source  = FileSpout(path)
    val builder = new EnronGraphBuilder()
    Raphtory.stream(spout = source, graphBuilder = builder).use { graph =>
      for {
        job1 <- IO.blocking {
                  graph
                    .at(989858340000L)
                    .past()
                    .execute(EdgeList())
                    .writeTo(PulsarSink("EdgeList"))
                }
        _    <- IO.blocking(job1.waitForJob())
        job2 <- IO.blocking {
                  graph
                    .range(963557940000L, 989858340000L, 1000000000)
                    .past()
                    .execute(ConnectedComponents())
                    .writeTo(PulsarSink("ConnectedComponents"))
                }

        _    <- IO.blocking(job2.waitForJob())
      } yield ExitCode.Success

    }

  }
}
