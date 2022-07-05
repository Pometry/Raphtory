package com.raphtory.internals.storage.pojograph.entities.external.vertex

import com.raphtory.api.analysis.visitor.HistoricEvent
import com.raphtory.internals.components.querymanager.GenericVertexMessage
import com.raphtory.internals.storage.pojograph.PojoGraphLens

import scala.reflect.ClassTag

abstract private[pojograph] class PojoVertexViewBase(vertex: PojoVertexBase) extends PojoVertexBase {
  override def lens: PojoGraphLens = vertex.lens

  override def Type(): String = vertex.Type()

  override def firstActivityAfter(time: Long, strict: Boolean): Option[HistoricEvent] =
    vertex.firstActivityAfter(time, strict)

  override def lastActivityBefore(time: Long, strict: Boolean): Option[HistoricEvent] =
    vertex.lastActivityBefore(time, strict)

  override def latestActivity(): HistoricEvent = vertex.latestActivity()

  override def earliestActivity(): HistoricEvent = vertex.earliestActivity()

  override def getPropertySet(): List[String] = vertex.getPropertySet()

  override def getPropertyAt[T](key: String, time: Long): Option[T] = vertex.getPropertyAt(key, time)

  override def getPropertyHistory[T](key: String, after: Long, before: Long): Option[List[(Long, T)]] =
    vertex.getPropertyHistory(key, after, before)

  override def history(): List[HistoricEvent] = vertex.history()

  override def active(after: Long, before: Long): Boolean = vertex.active(after, before)

  override def aliveAt(time: Long, window: Long): Boolean = vertex.aliveAt(time, window)
}

abstract private[pojograph] class PojoLocalVertexViewBase(val vertex: PojoVertexBase)
        extends PojoVertexViewBase(vertex) {
  override type IDType = vertex.IDType

  override def setState(key: String, value: Any): Unit = vertex.setState(key, value)

  override def getState[T](key: String, includeProperties: Boolean): T = vertex.getState(key, includeProperties)

  override def getStateOrElse[T](key: String, value: T, includeProperties: Boolean): T =
    vertex.getStateOrElse(key, value, includeProperties)

  override def containsState(key: String, includeProperties: Boolean): Boolean =
    vertex.containsState(key, includeProperties)

  override def getOrSetState[T](key: String, value: T, includeProperties: Boolean): T =
    vertex.getOrSetState(key, value, includeProperties)

  override def appendToState[T: ClassTag](key: String, value: T): Unit = vertex.appendToState(key, value)

  override def remove(): Unit = vertex.remove()

  override def isFiltered: Boolean = vertex.isFiltered

  implicit override val IDOrdering: Ordering[IDType] = vertex.IDOrdering

  override def ID: IDType = vertex.ID

  override def hasMessage: Boolean = vertex.hasMessage

  override def messageQueue[T]: List[T] = vertex.messageQueue

  override def clearMessageQueue(): Unit = vertex.clearMessageQueue()

  override def receiveMessage(msg: GenericVertexMessage[_]): Unit = vertex.receiveMessage(msg)

  override def executeEdgeDelete(): Unit = vertex.executeEdgeDelete()
}
