package com.raphtory.internals.management

import cats.effect.IO
import cats.effect.unsafe.IORuntime

import java.util.concurrent.CompletableFuture
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

private[raphtory] class Scheduler {
  //FIXME: wipe this class out as we move to cats-effect
  private val threads: Int = Runtime.getRuntime.availableProcessors() // CHANGE ME HERE :)

  implicit val runtime = IORuntime.global

  def executeInParallel(
      tasks: List[IO[Unit]],
      onSuccess: () => Unit,
      errorHandler: (Throwable) => Unit
  ): Unit =
    IO
      .parSequenceN(threads)(tasks)
      .unsafeRunSync()
}
