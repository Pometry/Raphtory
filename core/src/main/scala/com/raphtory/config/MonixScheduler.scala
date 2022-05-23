package com.raphtory.config

import com.raphtory.components.Component
import com.typesafe.scalalogging.Logger
import monix.execution.ExecutionModel.AlwaysAsyncExecution
import monix.execution.{Scheduler => MScheduler}
import monix.execution.UncaughtExceptionReporter
import org.slf4j.LoggerFactory

import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import scala.concurrent.duration.FiniteDuration

/** @note DoNotDocument */
private[raphtory] class MonixScheduler {
  private val threads: Int     = 8
  private var schedulerStarted = false
  private val logger: Logger   = Logger(LoggerFactory.getLogger(this.getClass))

  // Will schedule things with delays
  private val scheduledExecutor = Executors.newSingleThreadScheduledExecutor()

  // For actual execution of tasks
  private val executorService =
    MScheduler.computation(parallelism = threads, executionModel = AlwaysAsyncExecution)

  // Logs errors to stderr or something
  private val uncaughtExceptionReporter =
    UncaughtExceptionReporter(executorService.reportFailure)

  implicit val scheduler: MScheduler = MScheduler(
          scheduledExecutor,
          executorService,
          uncaughtExceptionReporter,
          AlwaysAsyncExecution
  )

  def execute[T](component: Component[T]): Unit =
    if (schedulerStarted)
      try scheduler.execute(() => component.run())
      catch {
        case e: RejectedExecutionException =>
          logger.error(s"Scheduler rejected scheduling of Component $component")
      }

  def scheduleOnce(delay: FiniteDuration, task: => Unit): Option[Cancelable] =
    if (schedulerStarted)
      try {
        val cancelable = scheduler.scheduleOnce(delay)(task)
        Some(() => cancelable.cancel())
      }
      catch {
        case e: RejectedExecutionException =>
          logger.error(s"Scheduler rejected scheduling of tast $task")
          None
      }
    else None

  def shutdown(): Unit = {
    schedulerStarted = false
    scheduledExecutor.shutdown()
    executorService.shutdown()
  }
}
