package com.raphtory.api.analysis.visitor

trait ReducedEdge extends ConcreteEdge[Long] {

  /** concrete type for exploded edge views of this edge which implements
    *  [[ExplodedEdge]] with same `IDType`
    */
  type ExplodedEdge <: ConcreteExplodedEdge[IDType]

  /** Return an [[ExplodedEdge]] instance for each time the edge is
    * active in the current view.
    */
  def explode(): List[ExplodedEdge]
}
