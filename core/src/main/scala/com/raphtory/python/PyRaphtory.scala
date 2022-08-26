package com.raphtory.python

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.Resource
import cats.effect.Sync
import cats.syntax.all._
import com.monovore.decline._
import com.monovore.decline.effect._
import com.raphtory.internals.management.Py4JServer
import com.raphtory.internals.management.PythonInterop
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import java.io._
import scala.concurrent.duration.DurationInt
import scala.jdk.OptionConverters._

object PyRaphtory
        extends CommandIOApp(
                name = "PyRaphtory",
                header = "Support for running supporting Python withing Raphtory",
                version = "0.1.0"
        ) {

  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  def checkParent(parentID: Long): IO[Unit] =
    IO.blocking(ProcessHandle.current().parent().toScala match {
      case None    =>
        logger.error("parent proces not found, shutting down")
        throw new RuntimeException("parent process not found")
      case Some(p) =>
        if (parentID == p.pid())
          logger.trace("process still alive")
        else {
          logger.error("parent process died, shutting down")
          throw new RuntimeException("parent process died")
        }
    }) *> IO.sleep(10.seconds)

  override def main: Opts[IO[ExitCode]] = {
    val parentID = Opts.option[Long](long = "parentID", short = "p", help = "Parent process ID").orNone
    val gateway  = Py4JServer.fromEntryPoint[IO](PythonInterop)

    parentID.map {
      case None     => gateway.useForever
      case Some(id) =>
        gateway.use(_ => IO.blocking(logger.trace("running PyRaphtory with parent check")) *> checkParent(id).foreverM)
    }
  }
}
