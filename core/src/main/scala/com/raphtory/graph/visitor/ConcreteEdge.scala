package com.raphtory.graph.visitor

/** @DoNotDocument */
trait ConcreteEdge[T] extends Edge {
  override type IDType = T
}
