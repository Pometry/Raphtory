package com.raphtory.storage.pojograph.entities.external

import com.raphtory.components.querymanager.FilteredInEdgeMessage
import com.raphtory.components.querymanager.FilteredOutEdgeMessage
import com.raphtory.components.querymanager.VertexMessage
import com.raphtory.graph.visitor.ConcreteEdge
import com.raphtory.graph.visitor.ConcreteExplodedEdge
import com.raphtory.graph.visitor.Edge
import com.raphtory.graph.visitor.ExplodedEdge
import com.raphtory.storage.pojograph.PojoGraphLens
import com.raphtory.storage.pojograph.entities.internal.PojoEdge
import com.raphtory.storage.pojograph.entities.internal.SplitEdge

/** !DoNotDocument */
class PojoExEdge(val edge: PojoEdge, id: Long, val view: PojoGraphLens)
        extends PojoExEntity(edge, view)
        with ConcreteEdge[Long] {

  override type ExplodedEdge = PojoExplodedEdge
  def ID() = id

  def src() = edge.getSrcId

  def dst() = edge.getDstId

  def send(data: Any): Unit =
    view.sendMessage(VertexMessage(view.superStep + 1, id, data))

  override def explode(): List[ExplodedEdge] =
    history().collect { case event if event.event => PojoExplodedEdge.fromEdge(this, event.time) }

  def isExternal: Boolean                    = edge.isInstanceOf[SplitEdge]

  override def remove(): Unit = {
    view.needsFiltering = true
    view.sendMessage(FilteredOutEdgeMessage(view.superStep + 1, src(), dst()))
    view.sendMessage(FilteredInEdgeMessage(view.superStep + 1, dst(), src()))
  }
}
