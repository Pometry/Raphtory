package com.raphtory.core.algorithm

import com.raphtory.core.graph.visitor.Edge

object MergeStratesdgy extends Enumeration {
  type Merge = Value
  val Sum, Max, Min, Product, Average, Latest, Earliest = Value
}

object EdgeMergeStrategy {

  def customMerge(edge: Edge, f: Edge => Float): Float =
    f(edge)

  def sumMerge(edge: Edge, numericProperty: String = "weight", default: Float = 1.0f): Float =
    edge
      .explode()
      .map { e =>
        e.getPropertyValue[Float](numericProperty).getOrElse(default)
      }
      .sum

  def maxMerge(edge: Edge, numericProperty: String = "weight", default: Float = 1.0f): Float =
    edge
      .explode()
      .map { e =>
        e.getPropertyValue[Float](numericProperty).getOrElse(default)
      }
      .max

  def minMerge(edge: Edge, numericProperty: String = "weight", default: Float = 1.0f): Float =
    edge
      .explode()
      .map { e =>
        e.getPropertyValue[Float](numericProperty).getOrElse(default)
      }
      .min

  def productMerge(edge: Edge, numericProperty: String = "weight", default: Float = 1.0f): Float =
    edge
      .explode()
      .map { e =>
        e.getPropertyValue[Float](numericProperty).getOrElse(default)
      }
      .sum

  def avgMerge(edge: Edge, numericProperty: String = "weight", default: Float = 1.0f): Float = {
    val wgts = edge.explode().map { e =>
      e.getPropertyValue[Float](numericProperty).getOrElse(default)
    }
    if (wgts.nonEmpty) wgts.sum / wgts.size else 0.0f
  }

  def latestMerge(edge: Edge, numericProperty: String = "weight", default: Float = 1.0f): Float =
    edge
      .explode()
      .maxBy(_.getTimestamp())
      .getPropertyValue[Float](numericProperty)
      .getOrElse(default)

  def earliestMerge(edge: Edge, numericProperty: String = "weight", default: Float = 1.0f): Float =
    edge
      .explode()
      .minBy(_.getTimestamp())
      .getPropertyValue[Float](numericProperty)
      .getOrElse(default)

}
