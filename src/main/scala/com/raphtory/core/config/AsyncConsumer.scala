package com.raphtory.core.config

import com.raphtory.core.components.Component

class AsyncConsumer[T](worker: Component[T]) extends Runnable {

  val monixScheduler = new MonixScheduler

  def run(): Unit =
    worker.consumer match {
      case Some(consumer) =>
        consumer.receiveAsync().thenApplyAsync { msg =>
          val reschedule = worker.handleMessage(msg)
          consumer.acknowledgeAsync(msg)
          if (reschedule)
            monixScheduler.scheduler.execute(this)
          else
            worker.stop()
        }
      case None           =>
        throw new IllegalStateException("Trying to consume from a Consumer that doesn't exist")
    }

  //        Async Batching:
  //        consumer.batchReceiveAsync().thenApplyAsync(msgs => {
  //          var reschedule = true
  //          var allReschedule = true
  //          while (msgs.iterator().hasNext) {
  //            val msg = msgs.iterator().next()
  //            reschedule = worker.handleMessage(msg)
  //            consumer.acknowledgeAsync(msg)
  //            if (reschedule == false) allReschedule = false
  //          }
  //          // all handlers return true -> reschedule, else stop the worker
  //          if (allReschedule) monixScheduler.scheduler.execute(this)
  //          else worker.stop()
  //        })

}

object AsyncConsumer {
  def apply[T](worker: Component[T]) = new AsyncConsumer[T](worker)
}
