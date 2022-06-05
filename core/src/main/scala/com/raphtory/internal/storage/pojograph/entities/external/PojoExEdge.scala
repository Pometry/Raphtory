package com.raphtory.internal.storage.pojograph.entities.external

import com.raphtory.api.analysis.visitor.ConcreteEdge
import com.raphtory.internal.components.querymanager.FilteredInEdgeMessage
import com.raphtory.internal.components.querymanager.FilteredOutEdgeMessage
import com.raphtory.internal.components.querymanager.VertexMessage
import com.raphtory.internal.storage.pojograph.PojoGraphLens
import com.raphtory.internal.storage.pojograph.entities.internal.PojoEdge
import com.raphtory.internal.storage.pojograph.entities.internal.SplitEdge

/** @note DoNotDocument */
class PojoExEdge(val edge: PojoEdge, id: Long, val view: PojoGraphLens)
        extends PojoExEntity(edge, view)
        with ConcreteEdge[Long] {

  override type ExplodedEdge = PojoExplodedEdge
  def ID: Long = id

  def src: Long = edge.getSrcId

  def dst: Long = edge.getDstId

  def send(data: Any): Unit =
    view.sendMessage(VertexMessage(view.superStep + 1, id, data))

  override def explode(): List[ExplodedEdge] =
    history().collect { case event if event.event => PojoExplodedEdge.fromEdge(this, event.time) }

  def isExternal: Boolean                    = edge.isInstanceOf[SplitEdge]

  override def remove(): Unit = {
    view.needsFiltering = true
    view.sendMessage(FilteredOutEdgeMessage(view.superStep + 1, src, dst))
    view.sendMessage(FilteredInEdgeMessage(view.superStep + 1, dst, src))
  }
}
