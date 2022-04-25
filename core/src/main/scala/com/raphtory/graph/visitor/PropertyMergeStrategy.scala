package com.raphtory.graph.visitor

import com.raphtory.util.Reduction._

/**
  * {s}`PropertyMergeStrategy`
  *  : Collection of pre-defined merge strategies.
  *
  * A merge strategy is a function that takes a collection of timestamped property values and returns a single value.
  * Merge strategies are used to control the aggregation semantics for property access in
  * [{s}`EntityVisitor`](com.raphtory.graph.visitor.EntityVisitor).
  * The general signature for a merge strategy is {s}`Seq[(Long, A)] => B`.
  *
  * ## Generic types
  *
  * {s}`PropertyMerge[A, B] = Seq[(Long, A)] => B`
  *   : Type of a merge strategy with return type {s}`B` for a property with value type {s}`A`
  *
  * ## Methods
  *
  * {s}`sum[T: Numeric]: PropertyMerge[T, T]`
  *  : Merge strategy that sums the property values
  *
  * {s}`max[T: Numeric]: PropertyMerge[T, T]`
  *  : Merge strategy that returns the maximum property value
  *
  * {s}`min[T: Numeric]: PropertyMerge[T, T]`
  *  : Merge strategy that returns the minimum property value
  *
  * {s}`product[T: Numeric]: PropertyMerge[T, T]`
  *  : Merge strategy that returns the product of property values
  *
  * {s}`average[T: Numeric]: PropertyMerge[T, Double]`
  *  : Merge strategy that returns the average of property values
  *
  * {s}`latest[T]: PropertyMerge[T, T]`
  *  : Merge strategy that returns the latest property value (i.e. the value corresponding to the largest timestamp)
  *    This is the default merge strategy for property access in Raphtory.
  *
  * {s}`earliest[T]: PropertyMerge[T, T]`
  *  : Merge startegy that returns the earliest property value (i.e., the value corresponding to the smallest timestamp)
  *
  * ```{seealso}
  * [](com.raphtory.graph.visitor.EntityVisitor),
  * [](com.raphtory.graph.visitor.Edge),
  * [](com.raphtory.graph.visitor.Vertex)
  * ```
  */
object PropertyMergeStrategy {
  type PropertyMerge[A, B] = Seq[(Long, A)] => B

  def sum[T: Numeric]: PropertyMerge[T, T] = (history: Seq[(Long, T)]) => history.map(_._2).sum

  def max[T: Numeric]: PropertyMerge[T, T] = (history: Seq[(Long, T)]) => history.map(_._2).max

  def min[T: Numeric]: PropertyMerge[T, T] = (history: Seq[(Long, T)]) => history.map(_._2).min

  def product[T: Numeric]: PropertyMerge[T, T] =
    (history: Seq[(Long, T)]) => history.map(_._2).product

  def average[T: Numeric]: PropertyMerge[T, Double] =
    (history: Seq[(Long, T)]) => history.map(_._2).mean

  def latest[T]: PropertyMerge[T, T] = (history: Seq[(Long, T)]) => history.maxBy(_._1)._2

  def earliest[T]: PropertyMerge[T, T] = (history: Seq[(Long, T)]) => history.minBy(_._1)._2

  def sequence[T]: PropertyMerge[T, Seq[T]] = (history: Seq[(Long, T)]) => history.map(_._2)
}
