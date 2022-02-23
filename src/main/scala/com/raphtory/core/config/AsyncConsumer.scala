package com.raphtory.core.config

import com.raphtory.core.components.Component

class AsyncConsumer[T](worker: Component[T]) extends Runnable {

  def run(): Unit =
    worker.cancelableConsumer match {
      case Some(consumer) =>
//        consumer.receiveAsync().thenApplyAsync { msg =>
//          val reschedule = worker.handleMessage(msg)
//          consumer.acknowledgeAsync(msg)
//          if (reschedule)
//            //monixScheduler.scheduler.execute(this)
//            worker.getScheduler().execute(this)
//          else
//            worker.stop()
//        }

        // Async Batching:
        consumer.batchReceiveAsync().thenApplyAsync { msgs =>
          var reschedule    = true
          var allReschedule = true
          while (msgs.iterator().hasNext) {
            val msg = msgs.iterator().next()
            reschedule = worker.handleMessage(msg)
            consumer.acknowledgeAsync(msg)
            if (!reschedule) allReschedule = false
          }

          // all handlers return true -> reschedule, else stop the worker
          if (allReschedule)
            worker.getScheduler().execute(this)
          else worker.stop()
        }
      case None           => throw new Error("Message handling consumer not initialised")
    }

}

object AsyncConsumer {
  def apply[T](worker: Component[T]) = new AsyncConsumer[T](worker)
}
