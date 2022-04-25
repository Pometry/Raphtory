package com.raphtory.config

import com.raphtory.components.Component

trait Connector {
  def listen[T](messageHandler: T => Unit, topic: ExclusiveTopic[T]): Unit
  def listen[T](messageHandler: T => Unit, topic: WorkPullTopic[T]): Unit
  def listen[T](messageHandler: T => Unit, topic: BroadcastTopic[T], partition: Int): Unit

  def endPoint[T](topic: CanonicalTopic[T]): EndPoint[T]
}
