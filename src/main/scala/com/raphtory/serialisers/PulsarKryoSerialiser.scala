package com.raphtory.serialisers

import com.twitter.chill.KryoPool
import com.twitter.chill.ScalaKryoInstantiator

import scala.reflect.runtime.universe._

/** @DoNotDocument */
class PulsarKryoSerialiser {
  private val kryo: KryoPool = ScalaKryoInstantiator.defaultPool

  def serialise[T](value: T): Array[Byte] = kryo.toBytesWithClass(value)

  def deserialise[T](bytes: Array[Byte]): T =
    kryo.fromBytes(bytes).asInstanceOf[T]
}

object PulsarKryoSerialiser {

  def apply(): PulsarKryoSerialiser =
    new PulsarKryoSerialiser()
}
