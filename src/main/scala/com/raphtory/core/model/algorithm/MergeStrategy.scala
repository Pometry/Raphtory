package com.raphtory.core.model.algorithm

object MergeStrategy extends Enumeration {
  type Merge = Value
  val Sum, Max, Min, Product, Difference = Value
}