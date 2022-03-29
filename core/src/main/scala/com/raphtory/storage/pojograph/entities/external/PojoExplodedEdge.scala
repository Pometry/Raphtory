package com.raphtory.storage.pojograph.entities.external

import com.raphtory.graph.visitor.ExplodedEdge
import com.raphtory.graph.visitor.HistoricEvent

/** @DoNotDocument */
class PojoExplodedEdge(objectEdge: PojoExEdge, override val timestamp: Long) extends ExplodedEdge {

  override def Type(): String =
    objectEdge.Type()

  override def ID(): Long =
    objectEdge.ID()

  override def src(): Long =
    objectEdge.src()

  override def dst(): Long =
    objectEdge.dst()

  override def getPropertySet(): List[String] =
    objectEdge.getPropertySet()

  override def send(data: Any): Unit =
    objectEdge.send(data)

  override def explode(): List[ExplodedEdge] = List(this)

  override def firstActivityAfter(time: Long): HistoricEvent = objectEdge.firstActivityAfter(time)

  override def lastActivityBefore(time: Long): HistoricEvent = objectEdge.lastActivityBefore(time)

  override def latestActivity(): HistoricEvent = objectEdge.latestActivity()

  override def earliestActivity(): HistoricEvent = objectEdge.earliestActivity()

  override def getPropertyAt[T](key: String, time: Long): Option[T] =
    objectEdge.getPropertyAt(key, time)

  override def getPropertyHistory[T](
      key: String,
      after: Long,
      before: Long = timestamp
  ): Option[List[(Long, T)]] = objectEdge.getPropertyHistory[T](key, after, before)

  override def history(): List[HistoricEvent] = objectEdge.history()

  override def active(after: Long, before: Long): Boolean = objectEdge.active(after, before)

  override def aliveAt(time: Long, window: Long): Boolean = objectEdge.aliveAt(time, window)
}
