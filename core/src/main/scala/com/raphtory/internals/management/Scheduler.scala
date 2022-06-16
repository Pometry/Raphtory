package com.raphtory.internals.management

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import com.raphtory.internals.components.Component
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

private[raphtory] class Scheduler {
  //FIXME: wipe this class out as we move to cats-effect
  private val threads: Int = 8

  implicit val runtime = IORuntime.global

  def execute[T](component: Component[T]): Unit =
    IO.delay(component.run()).unsafeToFuture()

  def scheduleOnce(delay: FiniteDuration, task: => Unit): () => Future[Unit] = {
    val (_, cancel) = (IO.sleep(delay) *> IO(task)).unsafeToFutureCancelable()
    cancel
  }

  def executeInParallel(
      tasks: Iterable[IO[Unit]],
      onSuccess: => Unit,
      errorHandler: (Throwable) => Unit
  ): () => Future[Unit] = {

    val (_, cancelable) = IO
      .parSequenceN(threads)(tasks.to(LazyList))
      .onError(t => IO(errorHandler(t)))
      .flatMap(_ => IO(onSuccess))
      .unsafeToFutureCancelable()

    cancelable
  }

}
