package com.raphtory.examples.lotr

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.examples.lotr.graphbuilders.LOTRGraphBuilder
import com.raphtory.sinks.FileSink
import com.raphtory.spouts.FileSpout
import com.raphtory.utils.FileUtils

import scala.language.postfixOps

object TutorialRunner extends IOApp {

  override def run(args: List[String]): IO[ExitCode] = {
    val path = "/tmp/lotr.csv"
    val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"

    FileUtils.curlFile(path, url)

    val source  = FileSpout(path)
    val builder = new LOTRGraphBuilder()
    Raphtory.load(spout = source, graphBuilder = builder).use { graph =>
      IO.blocking {

        val output = FileSink("/tmp/raphtory")

        graph
          .at(32674)
          .past()
          .execute(ConnectedComponents())
          .writeTo(output)
          .waitForJob()
        ExitCode.Success
      }
    }
  }

}
