package com.raphtory.core.model.graph.visitor

import com.raphtory.core.analysis.ObjectGraphLens
import com.raphtory.core.model.communication.VertexMessage
import com.raphtory.core.model.implementations.objectgraph.entities.internal.RaphtoryEdge

trait Edge extends EntityVisitor {

  //information about the edge meta data
  def ID():Long
  def src():Long
  def dst():Long

  //send a message to the vertex on the other end of the edge
  def send(data: Any): Unit


}
