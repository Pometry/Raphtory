package com.raphtory.graph.visitor

/**
  * {s}`ExplodedVertex`
  *  : Public interface for vertices in the multilayer view of the network
  */
trait ExplodedVertex extends Vertex {
  def timestamp: Long
}
