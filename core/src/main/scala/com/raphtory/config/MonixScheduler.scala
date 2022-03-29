package com.raphtory.config

import monix.execution.ExecutionModel.AlwaysAsyncExecution
import monix.execution.Scheduler
import monix.execution.UncaughtExceptionReporter

import java.util.concurrent.Executors

/** @DoNotDocument */
private[raphtory] class MonixScheduler {

  val threads: Int = 8

  // Will schedule things with delays
  val scheduledExecutor = Executors.newSingleThreadScheduledExecutor()

  // For actual execution of tasks
  val executorService =
    Scheduler.computation(parallelism = threads, executionModel = AlwaysAsyncExecution)

  // Logs errors to stderr or something
  val uncaughtExceptionReporter =
    UncaughtExceptionReporter(executorService.reportFailure)

  val scheduler = Scheduler(
          scheduledExecutor,
          executorService,
          uncaughtExceptionReporter,
          AlwaysAsyncExecution
  )
}
