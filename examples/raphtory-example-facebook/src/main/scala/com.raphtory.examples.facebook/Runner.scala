package com.raphtory.examples.facebook

import cats.effect.{ExitCode, IO, IOApp}
import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.{ConnectedComponents, EdgeList}
import com.raphtory.api.analysis.graphview.Alignment
import com.raphtory.examples.facebook.graphbuilders.FacebookGraphBuilder
import com.raphtory.sinks.PulsarSink
import com.raphtory.spouts.StaticGraphSpout

import java.io.File
import java.nio.file.{Files, Paths}
import scala.language.postfixOps
import scala.sys.process._

object Runner extends IOApp {

  // Create Graph

  override def run(args: List[String]): IO[ExitCode] = {

    if (!new File("/tmp", "facebook.csv").exists()) {
      val path = "/tmp/facebook.csv"
      try s"curl -o $path https://raw.githubusercontent.com/Raphtory/Data/main/facebook.csv " !!
      catch {
        case ex: Exception =>
          ex.printStackTrace()

          Files.deleteIfExists(Paths.get(path))
          throw ex
      }
    }

    val source: StaticGraphSpout = StaticGraphSpout("/tmp/facebook.csv")
    val builder                  = new FacebookGraphBuilder()
    Raphtory.load(spout = source, graphBuilder = builder).use { graph =>
      for {
        job1 <- IO.blocking(graph.at(88234).past().execute(EdgeList()).writeTo(PulsarSink("EdgeList")))
        _    <- IO.blocking(job1.waitForJob())
        job2 <- IO.blocking {
                  graph
                    .range(10000, 88234, 10000)
                    .window(List(500, 1000, 10000), Alignment.END)
                    .execute(ConnectedComponents())
                    .writeTo(PulsarSink("ConnectedComponents"))
                }
        _    <- IO.blocking(job2.waitForJob())
      } yield ExitCode.Success
    }
  }
}
