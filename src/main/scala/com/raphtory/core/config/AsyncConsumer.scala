package com.raphtory.core.config

import com.raphtory.core.components.Component

import scala.util.Random

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

        //      Async Batching:
//        println("hello")
//        consumer
//          .batchReceiveAsync()
//          .thenAccept { msgs =>
//            while (msgs.iterator().hasNext) {
//              val msg = msgs.iterator().next()
//              worker.handleMessage(msg)
//            }
//            consumer.acknowledgeAsync(msgs)
//          }
//          .whenComplete({
//            case (_, throwable) => worker.getScheduler().execute(this)
//          })
//          .exceptionally({
//            case throwable =>
//              println("hello")
//              null
//          })

        //.whenComplete()

//        var allReschedule = true
//
//        val messages = consumer.batchReceiveAsync().get()
//        messages.forEach { message =>
//          val reschedule = worker.handleMessage(message)
//          if (!reschedule) allReschedule = false
//          consumer.acknowledgeAsync(message)
//        }
//
//        // all handlers return true -> reschedule, else stop the worker
        val messages = consumer.batchReceive()
        messages.forEach { msg =>
          var reschedule    = true
          var allReschedule = true

          reschedule = worker.handleMessage(msg)

          if (!reschedule) allReschedule = false

        //all handlers return true -> reschedule, else stop the worker
        }
        consumer.acknowledgeAsync(messages)
        worker.getScheduler().execute(this)

      case None           => throw new Error("Message handling consumer not initialised")
    }

}

object AsyncConsumer {
  def apply[T](worker: Component[T]) = new AsyncConsumer[T](worker)
}
