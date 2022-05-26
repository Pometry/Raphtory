package com.raphtory.graph.visitor

/** A trait representing a view of an edge at a given point in time
  *
  * An exploded edge represents an Edge [[com.raphtory.graph.visitor.Edge]] at a particular time point
  * and combines the [[com.raphtory.graph.visitor.Edge]] and
  * ExplodedEntityVisitor [[com.raphtory.graph.visitor.ExplodedEntityVisitor]] traits.
  *
  * @see [[com.raphtory.graph.visitor.Edge]] [[com.raphtory.graph.visitor.ExplodedEntityVisitor]]
  */
trait ExplodedEdge extends Edge with ExplodedEntityVisitor
