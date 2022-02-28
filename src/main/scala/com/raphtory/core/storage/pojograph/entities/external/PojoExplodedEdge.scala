package com.raphtory.core.storage.pojograph.entities.external

import com.raphtory.core.graph.visitor.ExplodedEdge

class PojoExplodedEdge(objectEdge: PojoExEdge, timestamp: Long) extends ExplodedEdge {

  override def Type(): Unit =
    objectEdge.Type()

  override def ID(): Long =
    objectEdge.ID()

  override def src(): Long =
    objectEdge.src()

  override def dst(): Long =
    objectEdge.dst()

  override def getPropertySet(): List[String] =
    objectEdge.getPropertySet()

  override def getPropertyValue[T](key: String): Option[T] =
    objectEdge.getPropertyAt[T](key, timestamp)

  override def send(data: Any): Unit =
    objectEdge.send(data)

  override def getTimestamp(): Long = timestamp
}
