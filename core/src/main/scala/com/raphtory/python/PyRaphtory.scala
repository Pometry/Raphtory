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

  def checkAndWait(parentID: Long): IO[ExitCode] =
    for {
      parent <- IO.blocking(ProcessHandle.current().parent().toScala)
      code   <- parent match {
                  case None    =>
                    IO.blocking(
                            logger
                              .debug("check and wait but no parent")
                    ) *> IO.pure(ExitCode.Success)
                  case Some(p) =>
                    if (parentID == p.pid())
                      IO.blocking(
                              logger
                                .debug("check and wait with parent alive")
                      ) *> IO.sleep(10.seconds) *> checkAndWait(parentID)
                    else
                      IO.blocking(
                              logger
                                .debug("check and wait with parent dead")
                      ) *> IO.pure(ExitCode.Success)
                }
    } yield code

  override def main: Opts[IO[ExitCode]] = {
    val parentID = Opts.option[Long](long = "parentID", short = "p", help = "Parent process ID").orNone
    val gateway  = Py4JServer.fromEntryPoint[IO](PythonInterop)

    parentID.map {
      case None     => gateway.useForever
      case Some(id) =>
        gateway.use(_ => IO.println("running PyRaphtory with parent check") *> checkAndWait(id))
    }
  }
}
