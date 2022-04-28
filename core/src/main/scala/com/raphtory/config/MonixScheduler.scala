package com.raphtory.config

import com.raphtory.components.Component
import monix.execution.ExecutionModel.AlwaysAsyncExecution
import monix.execution.{Scheduler => MScheduler}
import monix.execution.UncaughtExceptionReporter

import java.util.concurrent.Executors
import scala.concurrent.duration.FiniteDuration

private[raphtory] class MonixScheduler extends Scheduler {
  val threads: Int = 8

  // Will schedule things with delays
  val scheduledExecutor = Executors.newSingleThreadScheduledExecutor()

  // For actual execution of tasks
  val executorService =
    MScheduler.computation(parallelism = threads, executionModel = AlwaysAsyncExecution)

  // Logs errors to stderr or something
  val uncaughtExceptionReporter =
    UncaughtExceptionReporter(executorService.reportFailure)

  val scheduler = MScheduler(
          scheduledExecutor,
          executorService,
          uncaughtExceptionReporter,
          AlwaysAsyncExecution
  )

  override def execute[T](component: Component[T]): Unit = scheduler.execute(() => component.run())

  override def scheduleOnce(delay: FiniteDuration, task: => Unit): Cancelable = {
    val cancelable = scheduler.scheduleOnce(delay)(task)
    () => cancelable.cancel()
  }
}
