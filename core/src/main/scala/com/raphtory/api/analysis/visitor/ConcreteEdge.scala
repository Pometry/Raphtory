package com.raphtory.api.analysis.visitor

/** [[Edge]] with parameterised `IDType`
  *
  * @tparam T `IDType` of the [[Edge]]
  */
trait ConcreteEdge[T] extends Edge {
  override type IDType = T
}
