package com.raphtory.internals.storage.arrow

import com.raphtory.internals.components.querymanager.GenericVertexMessage

import scala.collection.View

trait ArrowEntityStateRepository {

  def vertexVoted(): Unit

  def asGlobal(getDstVertex: Long): Long

  /**
   * copy and and empty the queue on the repository side
   *
   * @param vertexId
   * @tparam T
   * @return
   */
  def releaseQueue[T](vertexId: Long): View[T]

  def superStep: Int

  def getState[T](getLocalId: Long, key: String): Option[T]

  def getStateOrElse[T](getLocalId: Long, key: String, orElse: => T): T

  def setState(getLocalId: Long, key: String, value: Any): Unit

  def removeEdge(edgeId: Long): Unit
  def removeVertex(getLocalId: Long): Unit

  def hasMessage(getLocalId: Long): Boolean

  def sendMessage(msg: GenericVertexMessage[_]): Unit
}
