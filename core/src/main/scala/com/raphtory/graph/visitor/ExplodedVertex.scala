package com.raphtory.graph.visitor

/**
  * `ExplodedVertex`
  *  : Public interface for vertices in the multilayer view of the network
  *
  * An `ExplodedVertex` extends the [`Vertex`](com.raphtory.graph.visitor.Vertex) trait.
  *
  * ## Methods
  *
  * `name(nameProperty: String = "name")`: String
  *   : returns the name of the underlying vertex joined with the timestamp using `'_'`
  *
  * `baseName: String`
  *   : name of the underlying vertex
  *
  * `baseName(nameProperty: String): String`
  *   : use `nameProperty` instead of `"name"` to look up vertex name
  *
  * ```{seealso}
  * [](com.raphtory.graph.visitor.Vertex),
  * [](com.raphtory.graph.visitor.ExplodedEntityVisitor)
  * ```
  */
trait ExplodedVertex extends Vertex with ExplodedEntityVisitor {
  override type IDType = (Long, Long)
  override type Edge <: ConcreteExplodedEdge[IDType]
  override def name(nameProperty: String): String = s"${super.name(nameProperty)}_$timestamp"
  def baseName: String                            = super.name()
  def baseName(nameProperty: String): String      = super.name(nameProperty)
}
