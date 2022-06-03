package com.raphtory.api.visitor

/** @note DoNotDocument */
trait ConcreteEdge[T] extends Edge {
  override type IDType = T
}
