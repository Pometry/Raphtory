package com.raphtory.internals.storage.arrow

trait ArrowEntityStateRepository {
  def getState[T](getLocalId: Long, key: String): T

  def getStateOrElse[T](getLocalId: Long, key: String, orElse: => T): T

  def setState(getLocalId: Long, key: String, value: Any): Unit

  def removeEdge(edgeId: Long): Unit

  def hasMessage(getLocalId: Long): Boolean

}
