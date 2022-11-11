package com.raphtory.internals.components.querymanager

import com.google.protobuf.ByteString
import com.raphtory.internals.serialisers.KryoSerialiser
import scalapb.TypeMapper

import scala.util.Try

trait ProtoMessage[P <: ProtoDef, S] {
  private val kryo                   = Proto.kryo
  implicit val tms: TypeMapper[P, S] = TypeMapper(toScala)(toProto)
  protected def buildProto(bytes: ByteString): P
  private def toProto(message: S): P = buildProto(ByteString.copyFrom(kryo.serialise(message)))
  private def toScala(message: P): S = kryo.deserialise[S](message.bytes.toByteArray)
}

trait ProtoField[S] {
  private val kryo                            = Proto.kryo
  implicit val tmf: TypeMapper[ByteString, S] = TypeMapper(toScala)(toProto)
  private def toProto(message: S): ByteString = ByteString.copyFrom(kryo.serialise(message))
  private def toScala(bytes: ByteString): S   = kryo.deserialise[S](bytes.toByteArray)
}

trait TryProtoField[W, S] {
  private val kryo                            = Proto.kryo
  implicit val tmf: TypeMapper[ByteString, W] = TypeMapper(toScala)(toProto)
  private def toProto(message: W): ByteString = ByteString.copyFrom(kryo.serialise(getTry(message).get))
  private def toScala(bytes: ByteString): W   = buildScala(Try(kryo.deserialise[S](bytes.toByteArray)))
  def buildScala(value: Try[S]): W
  def getTry(wrapper: W): Try[S]
}

object Proto {
  val kryo: KryoSerialiser = KryoSerialiser()
}

trait ProtoDef extends {
  def bytes: ByteString
}
