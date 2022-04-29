package com.raphtory.config

import com.raphtory.components.Component

trait Connector {

  def register[T](
      id: String,
      messageHandler: T => Unit,
      topics: Seq[CanonicalTopic[T]]
  ): CancelableListener

  def endPoint[T](topic: CanonicalTopic[T]): EndPoint[T]
}
