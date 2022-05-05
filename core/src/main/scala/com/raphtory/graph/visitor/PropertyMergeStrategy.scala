package com.raphtory.graph.visitor

import com.raphtory.util.Reduction._

/** Collection of pre-defined merge strategies.
  *
  * A merge strategy is a function that takes a collection of timestamped property values and returns a single value.
  * Merge strategies are used to control the aggregation semantics for property access in
  * [`EntityVisitor`](com.raphtory.graph.visitor.EntityVisitor).
  * The general signature for a merge strategy is `Seq[(Long, A)] => B`.
  * @see [[com.raphtory.graph.visitor.EntityVisitor]]
  *      [[com.raphtory.graph.visitor.Edge]]
  *      [[com.raphtory.graph.visitor.Vertex]]
  */
object PropertyMergeStrategy {
  /** Type of a merge strategy with return type `B` for a property with value type `A` */
  type PropertyMerge[A, B] = Seq[(Long, A)] => B

  /** Merge strategy that sums the property values */
  def sum[T: Numeric]: PropertyMerge[T, T] = (history: Seq[(Long, T)]) => history.map(_._2).sum

  /** Merge strategy that returns the maximum property value */
  def max[T: Numeric]: PropertyMerge[T, T] = (history: Seq[(Long, T)]) => history.map(_._2).max

  /** Merge strategy that returns the minimum property value */
  def min[T: Numeric]: PropertyMerge[T, T] = (history: Seq[(Long, T)]) => history.map(_._2).min

  /** Merge strategy that returns the product of property values */
  def product[T: Numeric]: PropertyMerge[T, T] =
    (history: Seq[(Long, T)]) => history.map(_._2).product

  /** Merge strategy that returns the average of property values */
  def average[T: Numeric]: PropertyMerge[T, Double] =
    (history: Seq[(Long, T)]) => history.map(_._2).mean

  /** Merge strategy that returns the latest property value (i.e. the value corresponding to the largest timestamp)
    * This is the default merge strategy for property access in Raphtory. */
  def latest[T]: PropertyMerge[T, T] = (history: Seq[(Long, T)]) => history.maxBy(_._1)._2

  /** Merge startegy that returns the earliest property value (i.e., the value corresponding to the smallest timestamp) */
  def earliest[T]: PropertyMerge[T, T] = (history: Seq[(Long, T)]) => history.minBy(_._1)._2

  def sequence[T]: PropertyMerge[T, Seq[T]] = (history: Seq[(Long, T)]) => history.map(_._2)
}
