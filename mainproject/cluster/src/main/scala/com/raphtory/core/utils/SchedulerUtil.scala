package com.raphtory.core.utils

import akka.actor.Actor
import akka.actor.ActorContext
import akka.actor.ActorRef
import akka.actor.Cancellable

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

object SchedulerUtil {

  def scheduleTask(initialDelay: FiniteDuration, interval: FiniteDuration, receiver: ActorRef, message: Any)(
      implicit context: ActorContext,
      executor: ExecutionContext,
      sender: ActorRef = Actor.noSender
  ): Cancellable = {
    val scheduler = context.system.scheduler

    val cancellable = scheduler.schedule(initialDelay, interval, receiver, message)
    context.system.log.debug("The message [{}] has been scheduled for send to [{}].", message, receiver.path)

    cancellable
  }

  def scheduleTaskOnce(
      delay: FiniteDuration,
      receiver: ActorRef,
      message: Any
  )(implicit context: ActorContext, executor: ExecutionContext, sender: ActorRef = Actor.noSender): Cancellable = {
    val scheduler = context.system.scheduler

    val cancellable = scheduler.scheduleOnce(delay, receiver, message)
    context.system.log.debug("The message [{}] has been scheduled for send to [{}].", message, receiver.path)

    cancellable
  }

  def cancelTask(key: String, task: Cancellable)(implicit context: ActorContext): Boolean = {
    task.cancel()

    val isCancelled = task.isCancelled

    if (isCancelled)
      context.system.log.debug("The task [{}] has been cancelled.", key)
    else
      context.system.log.debug("Failed to cancel the task [{}].", key)

    isCancelled
  }

}
