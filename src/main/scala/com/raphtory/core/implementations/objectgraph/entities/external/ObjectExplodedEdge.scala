package com.raphtory.core.implementations.objectgraph.entities.external

import com.raphtory.core.model.graph.visitor.ExplodedEdge

class ObjectExplodedEdge(objectEdge: ObjectEdge,timestamp:Long) extends ExplodedEdge{

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
    objectEdge.getPropertyValueAt(key,timestamp)

  override def send(data: Any): Unit =
    objectEdge.send(data)
}
