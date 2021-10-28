package com.raphtory.core.model.graph.visitor

import com.raphtory.core.implementations.pojograph.PojoGraphLens
import com.raphtory.core.implementations.pojograph.entities.internal.RaphtoryEdge
import com.raphtory.core.model.graph.VertexMessage

trait Edge extends EntityVisitor {

  //information about the edge meta data
  def ID():Long
  def src():Long
  def dst():Long
  def explode():List[ExplodedEdge]

  //send a message to the vertex on the other end of the edge
  def send(data: Any): Unit


}
