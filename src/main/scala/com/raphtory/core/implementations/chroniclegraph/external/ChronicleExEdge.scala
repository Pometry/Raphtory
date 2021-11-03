package com.raphtory.core.implementations.chroniclegraph.external

import com.raphtory.core.implementations.chroniclegraph.entities.internal.ChronicleEdge
import com.raphtory.core.implementations.pojograph.PojoGraphLens
import com.raphtory.core.model.graph.VertexMessage
import com.raphtory.core.model.graph.visitor.{Edge, ExplodedEdge}

class ChronicleExEdge(edge: ChronicleEdge, id: Long, view: PojoGraphLens) extends ChronicleExEntity(edge,view) with Edge {
  def ID() = id

  def src() = edge.getSrcId

  def dst() = edge.getDstId

  def send(data: Any): Unit =
    view.sendMessage(VertexMessage(view.superStep+1,id, data))

  override def explode(): List[ExplodedEdge] = history().map( event => {
    new ChronicleExplodedEdge(this,event.time)
  })
}
