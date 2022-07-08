package com.raphtory.python

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.Resource
import cats.syntax.all._
import com.monovore.decline._
import com.monovore.decline.effect._
import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.api.analysis.algorithm.BaseAlgorithm
import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.algorithm.GenericallyApplicable
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.internals.management.python.UnsafeEmbeddedPythonProxy
import com.raphtory.sinks.FileSink
import com.raphtory.spouts.FileSpout
import com.raphtory.utils.FileUtils

import java.nio.file.Files
import java.nio.file.Path
import scala.concurrent.duration.FiniteDuration
import scala.io.Source

object PyRaphtory
        extends CommandIOApp(
                name = "PyRaphtory",
                header = "Support for running Raphtory locally for pyraphtory",
                version = "0.1.0"
        ) {

  override def main: Opts[IO[ExitCode]] = {
    val input = Opts
      .option[Path](long = "file", short = "f", help = "Input file for Python script")
      .validate("file does not exist")(Files.exists(_))

    val builder = Opts
      .option[String](long = "builder", short = "b", help = "Class for graph builder")

    val local       = Opts.flag("local", "Run Raphtory locally", "l").map(_ => Local)
    val distributed = Opts.flag("distributed", "Connect to existing Raphtory cluster", "d").map(_ => Distributed)

    val res: Opts[ConMode] = local orElse distributed
    val py                 = UnsafeEmbeddedPythonProxy()
    val builderPy          = UnsafeEmbeddedPythonProxy()

    val path = "/tmp/lotr.csv"
    val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"
    FileUtils.curlFile(path, url)

    (input, res, builder).tupled.map {
      case (file, Local, builderClass) =>
        (for {
          pyScript <- Resource.fromAutoCloseable(IO.blocking(Source.fromFile(file.toFile)))
          // ONE PYTHON FOR THE BUILDER
          script    = pyScript.mkString
          _        <- Resource.eval(IO.blocking(builderPy.run(script)))
          builder  <- Resource.eval(IO.blocking(builderPy.loadGraphBuilder[String](builderClass, None)))
          // ONE PYTHON FOR THE EVALUATOR
          _        <- Resource.eval(IO.blocking(py.run(script)))
          graph    <- Raphtory.loadIO(spout = FileSpout(path), graphBuilder = builder)
//          _        <- Resource.eval(IO.sleep(FiniteDuration(10, "s")))
          _        <- Resource.eval(IO.blocking(py.set("raphtory_graph", graph)))
          _        <- Resource.eval(IO.blocking(py.run("RaphtoryContext(TemporalGraph(raphtory_graph)).eval()")))
        } yield graph)
          .use { graph =>
            IO.sleep(FiniteDuration(50, "s")) *> IO.unit
          }
          .map(_ => ExitCode.Success)
    }
  }

}

sealed trait ConMode
case object Distributed extends ConMode
case object Local       extends ConMode
