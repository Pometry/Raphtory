package com.raphtory.internals.serialisers

import com.twitter.chill.ScalaKryoInstantiator

/** Support serialisation and deserialisation using ScalaKryoInstantiator from twitter.chill package
  *
  * Usage:
  *
  * {{{
  * import com.raphtory.serialisers.PulsarKryoSerialiser
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
  * }}}
  */
private[raphtory] class KryoSerialiser {
  private val kryo = ScalaKryoInstantiator.defaultPool

  /** serialise value to byte array
    * @param value value to serialise
    */
  def serialise[T](value: T): Array[Byte] = kryo.toBytesWithClass(value)

  /** deserialise byte array to object
    * @param bytes byte array to de-serialise
    */
  def deserialise[T](bytes: Array[Byte]): T =
    kryo.fromBytes(bytes).asInstanceOf[T]
}

private[raphtory] object KryoSerialiser {

  def apply(): KryoSerialiser =
    new KryoSerialiser()
}
