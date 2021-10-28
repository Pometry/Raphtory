package com.raphtory.core.implementations.pojograph.entities.external

import com.raphtory.core.model.graph.visitor.ExplodedEdge

class PojoExplodedEdge(objectEdge: PojoEdge, timestamp:Long) extends ExplodedEdge{

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

  override def getPropertyValue(key: String): Option[Any] =
    objectEdge.getPropertyAt(key,timestamp)

  override def send(data: Any): Unit =
    objectEdge.send(data)

  override def getTimestamp():Long = timestamp
}
