package com.raphtory.graph.visitor

/**
  * {s}`ExplodedVertex`
  *  : Public interface for vertices in the multilayer view of the network
  *
  * An {s}`ExplodedVertex` extends the [{s}`Vertex`](com.raphtory.graph.visitor.Vertex) trait.
  *
  * ## Methods
  *
  * {s}`name(nameProperty: String = "name")`: String
  *   : returns the name of the underlying vertex joined with the timestamp using {s}`'_'`
  *
  * {s}`baseName: String`
  *   : name of the underlying vertex
  *
  * {s}`baseName(nameProperty: String): String`
  *   : use {s}`nameProperty` instead of {s}`"name"` to look up vertex name
  */
trait ExplodedVertex extends Vertex with ExplodedEntityVisitor {
  override type VertexID = (Long, Long)
  override type Edge <: ExplodedEdge
  override def name(nameProperty: String): String = s"${super.name(nameProperty)}_$timestamp"
  def baseName: String                            = super.name()
  def baseName(nameProperty: String): String      = super.name(nameProperty)
}
