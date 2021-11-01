package com.raphtory.core.implementations.generic.entity.external

import com.raphtory.core.implementations.generic.GenericGraphLens
import com.raphtory.core.implementations.generic.entity.internal.InternalEdge
import com.raphtory.core.model.graph.VertexMessage
import com.raphtory.core.model.graph.visitor.{Edge, ExplodedEdge}

class GenericEdge(edge: InternalEdge, id: Long, view: GenericGraphLens) extends GenericEntity(edge,view) with Edge {
  def ID() = id

  def src() = edge.getSrcId

  def dst() = edge.getDstId

  def send(data: Any): Unit =
    view.sendMessage(VertexMessage(view.superStep+1,id, data))

  override def explode(): List[ExplodedEdge] = history().map( event => {
    new GenericExplodedEdge(this,event.time)
  })
}
