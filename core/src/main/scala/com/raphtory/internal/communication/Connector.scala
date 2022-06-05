package com.raphtory.internal.communication

/** @DoNotDocument */
trait Connector {

  def register[T](
      id: String,
      messageHandler: T => Unit,
      topics: Seq[CanonicalTopic[T]]
  ): CancelableListener

  def endPoint[T](topic: CanonicalTopic[T]): EndPoint[T]

  def shutdown(): Unit
}
