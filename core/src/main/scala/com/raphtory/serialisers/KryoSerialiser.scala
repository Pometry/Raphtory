package com.raphtory.serialisers

import com.twitter.chill.KryoPool
import com.twitter.chill.ScalaKryoInstantiator

import scala.reflect.runtime.universe._

/**
  *  {s}`KryoSerialiser()`
  *    : support serialisation and deserialisation using ScalaKryoInstantiator from twitter.chill package
  *
  *  ## Methods
  *
  *    {s}`serialise[T](value: T): Array[Byte]`
  *      : serialise value to byte array
  *
  *    {s}`deserialise[T](bytes: Array[Byte]): T`
  *      : deserialise byte array to object
  *
  *  Example Usage:
  *
  * ```{code-block} scala
  * import com.raphtory.serialisers.KryoSerialiser
  * import com.raphtory.config.PulsarController
  * import org.apache.pulsar.client.api.Schema
  *
  * val schema: Schema[Array[Byte]] = Schema.BYTES
  * val kryo = KryoSerialiser()
  *
  * val pulsarController = new PulsarController(config)
  * val client         = pulsarController.accessClient
  * val producer_topic = "test_lotr_graph_input_topic"
  * val producer       = client.newProducer(Schema.BYTES).topic(producer_topic).create()
  * producer.sendAsync(kryo.serialise("Gandalf,Benjamin,400"))
  * ```
  *
  * ```{seealso}
  * [](com.raphtory.client.RaphtoryClient)
  * ```
  */
class KryoSerialiser {
  private val kryo = ScalaKryoInstantiator.defaultPool

  def serialise[T](value: T): Array[Byte] = kryo.toBytesWithClass(value)

  def deserialise[T](bytes: Array[Byte]): T =
    kryo.fromBytes(bytes).asInstanceOf[T]
}

object KryoSerialiser {

  def apply(): KryoSerialiser =
    new KryoSerialiser()
}
