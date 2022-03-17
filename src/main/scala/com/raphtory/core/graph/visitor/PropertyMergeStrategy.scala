package com.raphtory.core.graph.visitor

import com.raphtory.util.Reduction._

/**
  * {s}`MergeStrategy`
  *  : Collection of pre-defined merge strategies.
  *
  * A merge strategy is a function that takes a collection of timestamped property values and returns a single value.
  * The general signature for a merge strategy is {s}`Seq[(Long, A)] => B`.
  *
  * ## Methods
  *
  * {s}`sum[T: Numeric](history: (history: Seq[(Long, T)]): T`
  *  : Return the sum of property values
  *
  * {s}`max[T: Numeric](history: (history: Seq[(Long, T)]): T`
  *  : Return the maximum property value
  *
  * {s}`min[T: Numeric](history: (history: Seq[(Long, T)]): T`
  *  : Return the minimum property value
  *
  * {s}`product[T: Numeric](history: (history: Seq[(Long, T)]): T`
  *  : Return the product of property values
  *
  * {s}`average[T: Numeric](history: (history: Seq[(Long, T)]): Double`
  *  : Return the average of property values
  *
  * {s}`latest[T](history: (history: Seq[(Long, T)]): T`
  *  : Return the latest property value (i.e. the value corresponding to the largest timestamp)
  *
  * {s}`earliest[T](history: (history: Seq[(Long, T)]): T`
  *  : Return the earliest property value (i.e., the value corresponding to the smallest timestamp)
  *
  * ```{seealso}
  * [](com.raphtory.core.graph.visitor.EntityVisitor),
  * [](com.raphtory.core.graph.visitor.Edge),
  * [](com.raphtory.core.graph.visitor.Vertex)
  * ```
  */
object PropertyMergeStrategy {
  type PropertyMerge[A, B] = Seq[(Long, A)] => B

  def sum[T: Numeric](history: Seq[(Long, T)]): T =
    history.map(_._2).sum

  def max[T: Numeric](history: Seq[(Long, T)]): T =
    history.map(_._2).max

  def min[T: Numeric](history: Seq[(Long, T)]): T =
    history.map(_._2).min

  def product[T: Numeric](history: Seq[(Long, T)]): T =
    history.map(_._2).product

  def average[T: Numeric](history: Seq[(Long, T)]): Double =
    history.map(_._2).mean

  def latest[T](history: Seq[(Long, T)]): T =
    history.maxBy(_._1)._2

  def earliest[T](history: Seq[(Long, T)]): T =
    history.minBy(_._1)._2

}
