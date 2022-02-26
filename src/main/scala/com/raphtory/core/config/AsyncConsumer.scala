package com.raphtory.core.config

import com.raphtory.core.components.Component

import scala.util.Random

class AsyncConsumer[S, R](worker: Component[S, R]) extends Runnable {

  def run(): Unit =
    worker.cancelableConsumer match {
      case Some(consumer) =>
        val message = consumer.receive()
        message.getValue

//        val messages = consumer.batchReceive()
//        messages.forEach { msg =>
//          var reschedule    = true
//          var allReschedule = true
//
//          reschedule = worker.handleMessage(msg)
//
//          if (!reschedule) allReschedule = false
//
//        //all handlers return true -> reschedule, else stop the worker
//        }
        //   consumer.acknowledgeAsync(messages)
        worker.getScheduler().execute(this)

      case None           => throw new Error("Message handling consumer not initialised")
    }

}

object AsyncConsumer {
  def apply[S, R](worker: Component[S, R]) = new AsyncConsumer[S, R](worker)
}
