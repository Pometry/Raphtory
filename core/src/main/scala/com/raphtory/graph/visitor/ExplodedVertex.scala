package com.raphtory.graph.visitor

/**
  * {s}`ExplodedVertex`
  *  : Public interface for vertices in the multilayer view of the network
  */
trait ExplodedVertex extends Vertex with ExplodedEntityVisitor

object ExplodedVertex {

  object InterlayerEdges extends Enumeration {
    type InterlayerEdges = Value
    val NEXT, ALL = Value
  }
}
