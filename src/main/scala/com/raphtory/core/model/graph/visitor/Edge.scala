package com.raphtory.core.model.graph.visitor

trait Edge extends EntityVisitor {

  //information about the edge meta data
  def ID():Long
  def src():Long
  def dst():Long
  def explode():List[ExplodedEdge]
  def weightOrHistory(weight : String="weight") : Float = {
    explode().map({
      e =>
        e.getPropertyValue[Float](weight).getOrElse(1.0f)
    }).sum
  }

  //send a message to the vertex on the other end of the edge
  def send(data: Any): Unit


}
