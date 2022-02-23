package com.raphtory.core.storage.pojograph.entities.external

import com.raphtory.core.components.querymanager.VertexMessage
import com.raphtory.core.graph.visitor.Edge
import com.raphtory.core.graph.visitor.ExplodedEdge
import com.raphtory.core.storage.pojograph.PojoGraphLens
import com.raphtory.core.storage.pojograph.entities.internal.PojoEdge

class PojoExEdge(edge: PojoEdge, id: Long, view: PojoGraphLens) extends PojoExEntity(edge, view) with Edge {

  def ID() = id

  def src() = edge.getSrcId

  def dst() = edge.getDstId

  def send(data: Any): Unit =
    view.sendMessage(VertexMessage(view.superStep + 1, id, data))

  override def explode(): List[ExplodedEdge] =
    history().map(event => new PojoExplodedEdge(this, event.time))
}
