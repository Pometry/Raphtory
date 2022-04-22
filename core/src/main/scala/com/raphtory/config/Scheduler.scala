package com.raphtory.config

import com.raphtory.components.Component

import scala.concurrent.duration.FiniteDuration

trait Scheduler {
  def execute[T](component: Component[T]): Unit
  def scheduleOnce(delay: FiniteDuration, task: => Unit): Cancelable
}
