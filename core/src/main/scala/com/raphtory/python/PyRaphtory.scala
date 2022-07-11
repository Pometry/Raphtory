package com.raphtory.python

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.Resource
import cats.syntax.all._
import com.monovore.decline._
import com.monovore.decline.effect._
import com.raphtory.Raphtory
import com.raphtory.api.querytracker.QueryProgressTracker
import com.raphtory.internals.management.PyRef
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
    // FIXME: these need lifting into IO but GraphBuilder is still outside of IO
    val evalPy             = UnsafeEmbeddedPythonProxy()
    val builderPy          = UnsafeEmbeddedPythonProxy()

    val path = "/tmp/lotr.csv"
    val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"
    FileUtils.curlFile(path, url)

    (input, res, builder).tupled.map {
      case (file, Local, builderClass) =>
        val builderIO = for {
          script  <- IO.fromEither(Using(Source.fromFile(file.toFile))(_.mkString).toEither)
          _       <- IO.blocking(builderPy.run(script))
          builder <- IO.blocking(builderPy.loadGraphBuilder[String](builderClass, None))
          _       <- IO.blocking(evalPy.run(script))
        } yield (builder, script)

        val mainRes = for {
          out              <- Resource.eval(builderIO)
          (builder, script) = out
          graph            <- Raphtory.loadIO(spout = FileSpout(path), graphBuilder = builder)
        } yield (graph, script)

        mainRes.use {
          case (graph, script) =>
            for {
              _       <- IO.blocking(evalPy.set("raphtory_graph", graph))
              _       <- IO.blocking(evalPy.set("py_script", script))
              _       <-
                IO.blocking(
                        evalPy.run(
                                "tracker = RaphtoryContext(rg = TemporalGraph(raphtory_graph), script=py_script).eval()"
                        )
                )
              tracker <- IO.blocking(evalPy.invoke(PyRef("tracker"), "inner_tracker")).map {
                           case qt: QueryProgressTracker => qt
                         }
              _       <- IO.blocking(tracker.waitForJob())
            } yield ExitCode.Success
        }

    }
  }

}

sealed trait ConMode
case object Distributed extends ConMode
case object Local       extends ConMode
