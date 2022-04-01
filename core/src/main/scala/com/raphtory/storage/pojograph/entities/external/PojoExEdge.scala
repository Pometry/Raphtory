package com.raphtory.storage.pojograph.entities.external

import com.raphtory.components.querymanager.VertexMessage
import com.raphtory.graph.visitor.Edge
import com.raphtory.graph.visitor.ExplodedEdge
import com.raphtory.storage.pojograph.PojoGraphLens
import com.raphtory.storage.pojograph.entities.internal.PojoEdge

/** @DoNotDocument */
class PojoExEdge(edge: PojoEdge, id: Long, view: PojoGraphLens)
        extends PojoExEntity(edge, view)
        with Edge {

  def ID() = id

  def src() = edge.getSrcId

  def dst() = edge.getDstId

  def send(data: Any): Unit =
    view.sendMessage(VertexMessage(view.superStep + 1, id, data))

  override def explode(): List[ExplodedEdge] =
    history().map(event => new PojoExplodedEdge(this, event.time))
}
