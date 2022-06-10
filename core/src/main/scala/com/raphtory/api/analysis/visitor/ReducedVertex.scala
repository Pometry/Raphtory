package com.raphtory.api.analysis.visitor

/** Vertex with concrete `Long` `IDType` (i.e. a non-exploded Vertex) */
trait ReducedVertex extends Vertex {
  override type IDType = Long
}
