package com.raphtory.python

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.Resource
import cats.syntax.all._
import com.monovore.decline._
import com.monovore.decline.effect._
import com.raphtory.internals.management.{Py4JServer, PythonInterop}

object PyRaphtory
        extends CommandIOApp(
                name = "PyRaphtory",
                header = "Support for running supporting Python withing Raphtory",
                version = "0.1.0"
        ) {

  override def main: Opts[IO[ExitCode]] = {
    for {
      gateway <- Py4JServer.fromEntryPoint[IO](PythonInterop)
      b <- IO.blocking(println(s"${gateway.getListeningPort}, ${Py4JServer.secret}")) >> IO.never
      c <- IO.never
    } yield ExitCode.Success
  }
