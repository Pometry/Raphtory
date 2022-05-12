package com.raphtory.graph.visitor

/** @note DoNotDocument */
trait ConcreteEdge[T] extends Edge {
  override type IDType = T
}
