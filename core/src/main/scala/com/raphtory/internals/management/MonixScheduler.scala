package com.raphtory.internals.management

import com.raphtory.internals.components.Component
import com.typesafe.scalalogging.Logger
import monix.eval.Task
import monix.execution.ExecutionModel.AlwaysAsyncExecution
import monix.execution.Cancelable
import monix.execution.UncaughtExceptionReporter
import monix.execution.{Scheduler => MScheduler}
import org.slf4j.LoggerFactory

import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import scala.concurrent.duration.FiniteDuration

private[raphtory] class MonixScheduler {
  private val threads: Int     = 8
  private var schedulerRunning = true
  private val logger: Logger   = Logger(LoggerFactory.getLogger(this.getClass))
  private val lock: Lock       = new ReentrantLock()

  // Will schedule things with delays
  private val scheduledExecutor = Executors.newSingleThreadScheduledExecutor()

  // For actual execution of tasks
  private val executorService =
    MScheduler.computation(parallelism = threads, executionModel = AlwaysAsyncExecution)

  // Logs errors to stderr or something
  private val uncaughtExceptionReporter =
    UncaughtExceptionReporter(executorService.reportFailure)

  implicit private val scheduler: MScheduler = MScheduler(
          scheduledExecutor,
          executorService,
          uncaughtExceptionReporter,
          AlwaysAsyncExecution
  )

  def execute[T](component: Component[T]): Unit = {
    lock.lock()
    if (schedulerRunning)
      try scheduler.execute(() => component.run())
      catch {
        case e: RejectedExecutionException =>
          logger.error(s"Scheduler rejected scheduling of Component $component")
      }
    lock.unlock()
  }

  def scheduleOnce(delay: FiniteDuration, task: => Unit): Option[Cancelable] = {
    lock.lock()
    if (schedulerRunning)
      try {
        val cancelable = scheduler.scheduleOnce(delay)(task)
        Some(() => cancelable.cancel())
      }
      catch {
        case e: RejectedExecutionException =>
          logger.error(s"Scheduler rejected scheduling of tast $task")
          None
      }
      finally lock.unlock()
    else {
      lock.unlock()
      None
    }
  }

  def executeInParallel(
      tasks: Iterable[Task[Unit]],
      onSuccess: => Unit,
      errorHandler: (Throwable) => Unit
  ): Unit = {
    lock.lock()
    if (schedulerRunning)
      Task
        .parSequenceUnordered(tasks)
        .runAsync {
          case Right(_)                   => onSuccess
          case Left(exception: Exception) => errorHandler(exception)
          case Left(exception: Throwable) => errorHandler(exception)
        }
    lock.unlock()
  }

  def shutdown(): Unit = {
    lock.lock()
    try {
      schedulerRunning = false
      scheduledExecutor.shutdown()
      executorService.shutdown()
    }
    catch {
      case e: Exception => logger.error(e.getMessage) //not sure if any errors can happen here?
    }
    finally lock.unlock()

  }
}
