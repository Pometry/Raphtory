package com.raphtory.core.graph.visitor

import com.raphtory.util.Reduction._

object VertexMergeStrategy {
  type VertexMerge[A, B] = Seq[A] => B

  def sum[T: Numeric](values: Seq[T]): T =
    values.sum

  def max[T: Numeric](values: Seq[T]): T =
    values.max

  def min[T: Numeric](values: Seq[T]): T =
    values.min

  def product[T: Numeric](values: Seq[T]): T =
    values.product

  def average[T: Numeric](values: Seq[T]): Double =
    values.mean
}
