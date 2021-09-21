package com.raphtory.core.implementations.objectgraph.entities.external

import com.raphtory.core.implementations.objectgraph.ObjectGraphLens
import com.raphtory.core.implementations.objectgraph.entities.internal.RaphtoryEdge
import com.raphtory.core.model.graph.VertexMessage
import com.raphtory.core.model.graph.visitor.Edge

class ObjectEdge(edge: RaphtoryEdge, id: Long, view: ObjectGraphLens) extends ObjectEntity(edge,view) with Edge {
  def ID() = id

  def src() = edge.getSrcId

  def dst() = edge.getDstId

  def send(data: Any): Unit =
    view.sendMessage(VertexMessage(id, data))


}
