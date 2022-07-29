package com.raphtory.api.analysis.visitor

import com.raphtory.utils.Reduction._

/** Collection of pre-defined merge strategies.
  *
  * A merge strategy is a function that takes a collection of timestamped property values and returns a single value.
  * Merge strategies are used to control the aggregation semantics for property access in
  * [[EntityVisitor]].
  *
  * The general signature for a merge strategy is `Iterable[PropertyValue[A]] => B`.
  * @see [[EntityVisitor]]
  *      [[Edge]]
  *      [[Vertex]]
  */
object PropertyMergeStrategy {

  /** Type of a merge strategy with return type `B` for a property with value type `A` */
  type PropertyMerge[A, B] = Iterable[PropertyValue[A]] => B

  /** Merge strategy that sums the property values */
  def sum[T: Numeric]: PropertyMerge[T, T] = (history: Iterable[PropertyValue[T]]) => history.map(_.value).sum

  /** Merge strategy that returns the maximum property value */
  def max[T: Numeric]: PropertyMerge[T, T] = (history: Iterable[PropertyValue[T]]) => history.map(_.value).max

  /** Merge strategy that returns the minimum property value */
  def min[T: Numeric]: PropertyMerge[T, T] = (history: Iterable[PropertyValue[T]]) => history.map(_.value).min

  /** Merge strategy that returns the product of property values */
  def product[T: Numeric]: PropertyMerge[T, T] =
    (history: Iterable[PropertyValue[T]]) => history.map(_.value).product

  /** Merge strategy that returns the average of property values */
  def average[T: Numeric]: PropertyMerge[T, Double] =
    (history: Iterable[PropertyValue[T]]) => history.map(_.value).mean

  /** Merge strategy that returns the latest property value (i.e. the value corresponding to the largest timestamp)
    * This is the default merge strategy for property access in Raphtory.
    */
  def latest[T]: PropertyMerge[T, T] = (history: Iterable[PropertyValue[T]]) => history.max.value

  /** Merge startegy that returns the earliest property value (i.e., the value corresponding to the smallest timestamp) */
  def earliest[T]: PropertyMerge[T, T] = (history: Iterable[PropertyValue[T]]) => history.min.value

  /** Return the sequence of property values */
  def sequence[T]: PropertyMerge[T, Iterable[T]] = (history: Iterable[PropertyValue[T]]) => history.map(_.value)
}
