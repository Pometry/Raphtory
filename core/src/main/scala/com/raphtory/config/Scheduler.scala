package com.raphtory.config

import com.raphtory.components.Component

import scala.concurrent.duration.FiniteDuration

/** @DoNotDocument */
trait Scheduler {
  def execute[T](component: Component[T]): Unit
  def scheduleOnce(delay: FiniteDuration, task: => Unit): Option[Cancelable]
  def shutdown(): Unit
}
