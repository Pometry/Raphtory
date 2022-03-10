package com.raphtory.core.graph.visitor

import PropertyMergeStrategy.PropertyMerge
import com.raphtory.algorithms.generic.centrality.WeightedPageRank

trait Edge extends EntityVisitor {

  //information about the edge meta data
  def ID(): Long
  def src(): Long
  def dst(): Long
  def explode(): List[ExplodedEdge]

  def weight[A, B](
      weightProperty: String = "weight",
      mergeStrategy: PropertyMerge[A, B],
      default: A
  ): B =
    getProperty(weightProperty, mergeStrategy) match {
      case Some(value) => value
      case None        => mergeStrategy(history().filter(_.event).map(p => (p.time, default)))
    }

  def weight[A, B](weightProperty: String, mergeStrategy: PropertyMerge[A, B])(implicit
      numeric: Numeric[A]
  ): B = weight(weightProperty, mergeStrategy, numeric.one)

  def weight[A, B](mergeStrategy: PropertyMerge[A, B])(implicit
      numeric: Numeric[A]
  ): B = weight[A, B]("weight", mergeStrategy, numeric.one)

  def weight[A: Numeric](weightProperty: String, default: A): A =
    weight(weightProperty, PropertyMergeStrategy.sum[A], default)

  def weight[A: Numeric](default: A): A =
    weight("weight", PropertyMergeStrategy.sum[A], default)

  def weight[A](weightProperty: String)(implicit numeric: Numeric[A]): A =
    weight(weightProperty, PropertyMergeStrategy.sum[A], numeric.one)

  def weight[A]()(implicit numeric: Numeric[A]): A =
    weight("weight", PropertyMergeStrategy.sum[A], numeric.one)

  //send a message to the vertex on the other end of the edge
  def send(data: Any): Unit

}
