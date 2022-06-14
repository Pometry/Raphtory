package com.raphtory.internals.storage.pojograph.entities.external

import com.raphtory.api.analysis.visitor.ConcreteEdge
import com.raphtory.internals.components.querymanager.FilteredInEdgeMessage
import com.raphtory.internals.components.querymanager.FilteredOutEdgeMessage
import com.raphtory.internals.components.querymanager.VertexMessage
import com.raphtory.internals.storage.pojograph.PojoGraphLens
import com.raphtory.internals.storage.pojograph.entities.internal.PojoEdge
import com.raphtory.internals.storage.pojograph.entities.internal.SplitEdge

private[raphtory] class PojoExEdge(
    val edge: PojoEdge,
    id: Long,
    val view: PojoGraphLens,
    start: Long,
    end: Long
) extends PojoExEntity(edge, view, start, end)
        with ConcreteEdge[Long] {

  def this(entity: PojoEdge, id: Long, view: PojoGraphLens) = {
    this(entity, id, view, view.start, view.end)
  }

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

  def viewBetween(after: Long = view.start, before: Long = view.end) =
    new PojoExEdge(edge, id, view, math.max(after, view.start), math.min(before, view.end))
}
