package com.raphtory.python

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.Resource
import cats.syntax.all._
import com.monovore.decline._
import com.monovore.decline.effect._
import com.raphtory.Raphtory
import com.raphtory.internals.management.python.UnsafeEmbeddedPythonProxy
import com.raphtory.spouts.FileSpout
import com.raphtory.utils.FileUtils

import java.nio.file.Files
import java.nio.file.Path
import scala.io.Source
import scala.util.Using

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

    val path = "/tmp/lotr.csv"
    val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"
    FileUtils.curlFile(path, url)

    (input, res, builder).tupled.map {
      case (file, Local, builderClass) =>
        (for {
          pyScript <- Resource.fromAutoCloseable(IO.blocking(Source.fromFile(file.toFile)))
          _        <- Resource.eval(IO.blocking(py.run(pyScript.mkString)))
          builder  <- Resource.eval(IO.blocking(py.loadGraphBuilder[String](builderClass, None)))
          graph    <- Raphtory.loadIO(spout = FileSpout(path), graphBuilder = builder)
          _        <- Resource.eval(IO.blocking(py.set("raphtory_graph", graph)))
          _ <- Resource.eval(IO.blocking(py.run("RaphtoryContext(TemporalGraph(raphtory_graph)).eval()")))
        } yield ()).use(_ => IO.pure(ExitCode.Success))
    }
  }

}

sealed trait ConMode
case object Distributed extends ConMode
case object Local       extends ConMode
