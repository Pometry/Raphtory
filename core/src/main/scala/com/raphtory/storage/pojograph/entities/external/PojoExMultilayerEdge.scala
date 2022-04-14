package com.raphtory.storage.pojograph.entities.external

import com.raphtory.components.querymanager.FilteredInEdgeMessage
import com.raphtory.components.querymanager.FilteredOutEdgeMessage
import com.raphtory.components.querymanager.VertexMessage
import com.raphtory.graph.visitor.ConcreteExplodedEdge
import com.raphtory.graph.visitor.EntityVisitor
import com.raphtory.graph.visitor.ExplodedEdge
import com.raphtory.graph.visitor.HistoricEvent
import com.raphtory.storage.pojograph.PojoGraphLens

class PojoExMultilayerEdge(
    override val timestamp: Long,
    override val ID: (Long, Long),
    override val src: (Long, Long),
    override val dst: (Long, Long),
    protected val edge: EntityVisitor,
    protected val view: PojoGraphLens
) extends ConcreteExplodedEdge[(Long, Long)] {
  override type ExplodedEdge = PojoExMultilayerEdge
  override def explode(): List[ExplodedEdge] = List(this)

  override def send(data: Any): Unit = VertexMessage(view.superStep + 1, ID, data)

  override def Type(): String = edge.Type()

  override def firstActivityAfter(time: Long): Option[HistoricEvent] = edge.firstActivityAfter(time)

  override def lastActivityBefore(time: Long): Option[HistoricEvent] =
    edge.lastActivityBefore(time)

  override def latestActivity(): HistoricEvent = edge.latestActivity()

  override def earliestActivity(): HistoricEvent = edge.earliestActivity()

  override def getPropertySet(): List[String] = edge.getPropertySet()

  override def getPropertyAt[T](key: String, time: Long): Option[T] = edge.getPropertyAt(key, time)

  override def getPropertyHistory[T](
      key: String,
      after: Long,
      before: Long = timestamp
  ): Option[List[(Long, T)]] = edge.getPropertyHistory[T](key, after, before)

  override def history(): List[HistoricEvent] = edge.history()

  override def active(after: Long, before: Long): Boolean = edge.active(after, before)

  override def aliveAt(time: Long, window: Long): Boolean = edge.aliveAt(time, window)

  override def remove(): Unit = {
    view.needsFiltering = true
    view.sendMessage(FilteredInEdgeMessage(view.superStep + 1, dst, src))
    view.sendMessage(FilteredOutEdgeMessage(view.superStep + 1, src, dst))
  }
}
