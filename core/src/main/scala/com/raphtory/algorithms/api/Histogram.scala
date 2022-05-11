package com.raphtory.algorithms.api

abstract class Histogram[T: Numeric](noBins: Int, minValue: T, maxValue: T) extends {
  protected val bins: Array[Int] = Array.fill(noBins)(0)

  def minimumValue(): T = minValue
  def maximumValue(): T = maxValue

  def cumSum(): Array[Int]
  def quantile(percentile: Float): Float
  def getBins: Array[Int] = bins
  def getBin(value: T): Int
  def getBinCount(value: T): Int
  def +=(newValue: T): Unit

}
