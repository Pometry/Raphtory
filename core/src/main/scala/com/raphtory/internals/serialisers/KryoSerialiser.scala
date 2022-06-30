package com.raphtory.internals.serialisers

import com.esotericsoftware.kryo.Serializer
import com.raphtory.internals.components.querymanager.DynamicLoader
import com.twitter.chill.AllScalaRegistrar
import com.twitter.chill.EmptyScalaKryoInstantiator
import com.twitter.chill.KryoBase
import com.twitter.chill.KryoPool

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
  private val kryo: KryoPool = ScalaKryoMaker.defaultPool

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

private object ScalaKryoMaker extends Serializable {
  private val mutex                      = new AnyRef with Serializable // some serializable object
  @transient private var kpool: KryoPool = null

  /**
    * Return a KryoPool that uses the ScalaKryoInstantiator
    */
  def defaultPool: KryoPool =
    mutex.synchronized {
      if (null == kpool)
        kpool = KryoPool.withByteArrayOutputStream(guessThreads, new CustomScalaKryoInstantiator)
      kpool
    }

  private def guessThreads: Int = {
    val cores                  = Runtime.getRuntime.availableProcessors
    val GUESS_THREADS_PER_CORE = 4
    GUESS_THREADS_PER_CORE * cores
  }
}

class CustomScalaKryoInstantiator extends EmptyScalaKryoInstantiator {

  override def newKryo: KryoBase = {
    val k       = super.newKryo
    k.setClassLoader(DynamicClassLoader(k.getClassLoader))
    val reg     = new AllScalaRegistrar
    reg(k)
    val default = k.getDefaultSerializer(classOf[DynamicLoader]).asInstanceOf[Serializer[DynamicLoader]]
    k.register(classOf[DynamicLoader], new DynamicLoaderSerializer(default))
    k
  }
}
