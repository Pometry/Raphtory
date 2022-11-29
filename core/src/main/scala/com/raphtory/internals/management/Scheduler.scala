package com.raphtory.internals.management

import cats.effect.IO
import cats.effect.unsafe.IORuntime

import java.util.concurrent.CompletableFuture
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

private[raphtory] class Scheduler {
  //FIXME: wipe this class out as we move to cats-effect
  private val threads: Int = 1 // Used to be 16

  implicit val runtime = IORuntime.global

//  def execute[T](component: Component[T]): Unit =
//    IO.blocking(component.run()).unsafeToFuture()

  def executeCompletable[T](task: => Unit): CompletableFuture[Unit] =
    IO.blocking(task).unsafeToCompletableFuture()

  def scheduleOnce(delay: FiniteDuration, task: => Unit): () => Future[Unit] = {
    val (_, cancel) = (IO.sleep(delay) *> IO.blocking(task)).unsafeToFutureCancelable()
    cancel
  }

//  def executeInParallel(
//      tasks: List[IO[Unit]],
//      onSuccess: () => Unit,
//      errorHandler: (Throwable) => Unit
//  ): () => Future[Unit] = {
//
//    val (_, cancelable) = IO
//      .parSequenceN(threads)(tasks)
//      .onError(t => IO.blocking(errorHandler(t)))
//      .flatMap(_ => IO.blocking(onSuccess()))
//      .unsafeToFutureCancelable()
//
//    cancelable
//  }

  def executeInParallel(
      tasks: List[IO[Unit]],
      onSuccess: () => Unit,
      errorHandler: (Throwable) => Unit
  ): Unit =
    IO
      .parSequenceN(threads)(tasks)
      .unsafeRunSync()
}
