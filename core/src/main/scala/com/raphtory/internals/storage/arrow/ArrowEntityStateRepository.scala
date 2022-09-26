package com.raphtory.internals.storage.arrow

import com.raphtory.internals.components.querymanager.GenericVertexMessage

import scala.collection.View

trait ArrowEntityStateRepository {
  def queue[T](getLocalId: Long): View[T]

  def superStep: Int

  def getState[T](getLocalId: Long, key: String): T

  def getStateOrElse[T](getLocalId: Long, key: String, orElse: => T): T

  def setState(getLocalId: Long, key: String, value: Any): Unit

  def removeEdge(edgeId: Long): Unit
  def removeVertex(getLocalId: Long): Unit

  def hasMessage(getLocalId: Long): Boolean

  def sendMessage(msg: GenericVertexMessage[_]): Unit
}
