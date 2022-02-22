package com.raphtory.serialisers

import com.twitter.chill.KryoPool
import com.twitter.chill.ScalaKryoInstantiator

import scala.reflect.runtime.universe._

class PulsarKryoSerialiser {
  private val kryo: KryoPool = ScalaKryoInstantiator.defaultPool

  def serialise(value: Any): Array[Byte] =
    value match {
      case s: String      => s.getBytes()
      case b: Array[Byte] => b
      case _              => kryo.toBytesWithClass(value)
    }

  def deserialise[T: TypeTag](bytes: Array[Byte]): T = {
    val data: Any = typeOf[T] match {
      case a if a =:= typeOf[Array[Byte]] => bytes
      case s if s =:= typeOf[String]      => new String(bytes, "UTF-8")
      case _                              => kryo.fromBytes(bytes)
    }

    data.asInstanceOf[T]
  }
}

object PulsarKryoSerialiser {

  def apply(): PulsarKryoSerialiser =
    new PulsarKryoSerialiser()
}
