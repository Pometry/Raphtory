package com.raphtory.core.model.graph.visitor

trait Edge extends EntityVisitor {

  //information about the edge meta data
  def ID():Long
  def src():Long
  def dst():Long
  def explode():List[ExplodedEdge]

  //send a message to the vertex on the other end of the edge
  def send(data: Any): Unit


}
