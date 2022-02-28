package com.raphtory.core.config

import com.raphtory.core.components.Component
import com.raphtory.serialisers.PulsarKryoSerialiser

import scala.collection.parallel.mutable.ParArray
import scala.reflect.ClassTag
import scala.util.Random
import scala.reflect.runtime.universe.TypeTag

class AsyncConsumer[S, R: TypeTag: ClassTag](worker: Component[S, R]) extends Runnable {

  val kryo                                           = PulsarKryoSerialiser()
  def deserialise[T: TypeTag](bytes: Array[Byte]): T = kryo.deserialise[T](bytes)

  def run(): Unit =
    worker.consumer match {
      case Some(consumer) =>
        val message = consumer.receive()
        val time    = System.currentTimeMillis()
        deserialise[Any](message.getValue) match {
          case batch: Array[Array[Byte]] =>
            batch
              .map(msg => deserialise[R](msg))
              .seq
              .foreach(msg => worker.handleMessage(msg))
          case batch: Array[R]           =>
            batch.seq
              .foreach(msg => worker.handleMessage(msg))
          case message: R                => worker.handleMessage(message)
        }
        worker.logger.trace(
                s"Deserialising message for ${worker.name()} took ${System.currentTimeMillis() - time}"
        )
        worker.getScheduler().execute(this)
        consumer.acknowledge(message)

      case None           => throw new Error("Message handling consumer not initialised")
    }

}

object AsyncConsumer {
  def apply[S, R: TypeTag: ClassTag](worker: Component[S, R]) = new AsyncConsumer[S, R](worker)
}
