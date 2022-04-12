package com.raphtory.config

import com.raphtory.components.Component

import scala.concurrent.duration.FiniteDuration

trait Scheduler {
  def execute(component: Component): Unit
  def scheduleOnce(delay: FiniteDuration, task: => Unit): Cancelable
}
