package com.raphtory.config

import akka.actor.typed.ActorSystem
import akka.actor.typed.SpawnProtocol
import com.raphtory.components.Component

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

/** @DoNotDocument */
class AkkaScheduler(system: ActorSystem[SpawnProtocol.Command]) extends Scheduler {

  override def execute[T](component: Component[T]): Unit =
    system.executionContext.execute(() => component.run())

  override def scheduleOnce(delay: FiniteDuration, task: => Unit): Cancelable = {
    implicit val ec: ExecutionContext = system.executionContext
    val cancellable                   =
      system.scheduler.scheduleOnce(delay, () => task) // TODO: is this really working?
    new Cancelable {
      override def cancel(): Unit = cancellable.cancel()
    }
  }
}
