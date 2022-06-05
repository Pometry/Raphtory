package com.raphtory.api.analysis.visitor

/** @note DoNotDocument */
trait ConcreteEdge[T] extends Edge {
  override type IDType = T
}
