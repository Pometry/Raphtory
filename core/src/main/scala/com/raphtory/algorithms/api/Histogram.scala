package com.raphtory.algorithms.api

/**
  * Public interface for the Histograms API within Graph State.
  *
  *  A Histogram maintains the distribution of a graph quantity in bins between the minimum and maximum value that
  *  this quantity takes.
  *
  * @see
  * [[com.raphtory.algorithms.api.GraphState]], [[com.raphtory.algorithms.generic.filters.EdgeQuantileFilter]], [[com.raphtory.algorithms.generic.filters.VertexQuantileFilter]]
  */

abstract class Histogram[T: Numeric](val minValue: T, val maxValue: T) {
  /** Return the total population size
    */
  def totalCount: Int

  /** Compute the cumulative density function of the property histogram unnormalised (in raw counts so adds to `totalCount`)
    */
  def cumSum(): Array[Int]

  /** Cumulative density function normalised */
  def cdf: Array[Float] = pdf.scanLeft(0.0f)(_+_)

  /** Probability density function */
  def pdf: Array[Float] = getBins.map(_/totalCount)

  /** Compute the value associated with a given quantile
    *  @param quantile  quantile to find. For example, 0.5 would correspond to the median value.
    */
  def quantile(percentile: Float): Float

  /** Getter function for obtaining the bins counts of the histogram.
    */
  def getBins: Array[Int]

  /** Get the bin associated with a given value. */
  def getBin(value: T): Int

  /** Find the population of a bin associated with a given value. */
  def getBinCount(value: T): Int
}
