package com.raphtory.config

import com.raphtory.components.Component

trait Connector {
  def registerExclusiveListener[T](component: Component[T], address: String): Unit
  def registerWorkPullListener[T](component: Component[T], address: String): Unit
  def registerBroadcastListener[T](component: Component[T], address: String, partition: Int): Unit

  def createExclusiveEndPoint[T](address: String): EndPoint[T]
  def createWorkPullEndPoint[T](address: String): EndPoint[T]
  def createBroadcastEndPoint[T](address: String): EndPoint[T]
}
