package com.raphtory.api.visitor

/** Public interface for vertices in the multilayer view of the network
  * An `ExplodedVertex` extends the [`Vertex`](com.raphtory.graph.visitor.Vertex) trait.
  * @see [[Vertex]] [[ExplodedEntityVisitor]]
  */
trait ExplodedVertex extends Vertex with ExplodedEntityVisitor {
  override type IDType = (Long, Long)
  override type Edge <: ConcreteExplodedEdge[IDType]

  /** returns the name of the underlying vertex joined with the timestamp using `_` */
  override def name(nameProperty: String): String = s"${super.name(nameProperty)}_$timestamp"

  /** name of the underlying vertex */
  def baseName: String = super.name()

  /** use `nameProperty` instead of `"name"` to look up vertex name */
  def baseName(nameProperty: String): String = super.name(nameProperty)
}
