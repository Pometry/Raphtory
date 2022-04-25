package com.raphtory.storage.pojograph.entities.external

import com.raphtory.components.querymanager.FilteredInEdgeMessage
import com.raphtory.components.querymanager.FilteredOutEdgeMessage
import com.raphtory.components.querymanager.VertexMessage
import com.raphtory.graph.visitor.ConcreteExplodedEdge
import com.raphtory.graph.visitor.ExplodedEdge
import com.raphtory.graph.visitor.HistoricEvent
import com.raphtory.storage.pojograph.PojoGraphLens
import com.raphtory.storage.pojograph.entities.internal.PojoEdge

/** @DoNotDocument */
class PojoExplodedEdge(
    objectEdge: PojoEdge,
    view: PojoGraphLens,
    id: Long,
    val src: Long,
    val dst: Long,
    override val timestamp: Long
) extends PojoExEntity(objectEdge, view)
        with ConcreteExplodedEdge[Long] {
  override type ExplodedEdge = PojoExplodedEdge
  override def ID(): Long = id

  override def explode(): List[ExplodedEdge] = List(this)

  override def send(data: Any): Unit = view.sendMessage(VertexMessage(view.superStep + 1, id, data))

  override def getPropertyHistory[T](
      key: String,
      after: Long,
      before: Long = timestamp
  ): Option[List[(Long, T)]] = super.getPropertyHistory(key, after, before)

  override def remove(): Unit = {
    view.needsFiltering = true
    view.sendMessage(FilteredOutEdgeMessage(view.superStep + 1, src, dst))
    view.sendMessage(FilteredInEdgeMessage(view.superStep + 1, dst, src))
  }
}

object PojoExplodedEdge {

  def fromEdge(pojoExEdge: PojoExEdge, timestamp: Long): PojoExplodedEdge =
    new PojoExplodedEdge(
            pojoExEdge.edge,
            pojoExEdge.view,
            pojoExEdge.ID(),
            pojoExEdge.src(),
            pojoExEdge.dst(),
            timestamp
    )
}
