package com.raphtory.api.analysis.visitor

/** A trait representing a view of an edge at a given point in time
  *
  * An exploded edge represents an Edge [[Edge]] at a particular time point
  * and combines the [[Edge]] and
  * ExplodedEntityVisitor [[ExplodedEntityVisitor]] traits.
  *
  * @see [[Edge]] [[ExplodedEntityVisitor]]
  */
trait ExplodedEdge extends Edge with ExplodedEntityVisitor {

  override def weight[A: Numeric](weightProperty: String, default: A): A =
    weight(weightProperty, PropertyMergeStrategy.latest[A], default)
}
