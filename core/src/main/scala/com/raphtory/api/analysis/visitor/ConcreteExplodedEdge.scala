package com.raphtory.api.analysis.visitor

/** [[ExplodedEdge]] with parameterised `IDType`
  *
  * @tparam T `IDType` of the [[ExplodedEdge]]
  */
trait ConcreteExplodedEdge[T] extends ExplodedEdge with ConcreteEdge[T]
