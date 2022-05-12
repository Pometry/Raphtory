package com.raphtory.algorithms.api

/**
  *  {s}`Histogram[T:Numeric](noBins: Int, minValue: T, maxValue: T) `
  *    : Public interface for the Histograms API within Graph State.
  *
  *    A Histogram maintains the distribution of a graph quantity in bins between the minimum and maximum value that
  *    this quantity takes.
  *
  *  ## Parameters
  *   {s}`noBins: Int = 1000`
  *     : Number of bins to be used in the histogram. The more the bins, the more precise the quantiles can be (depending on the underlying
  *     distribution) but the bigger the array being maintained in the graph state.
  *
  *   {s}`minValue: T`
  *     : Value corresponding to the lower bound of the leftmost bin (sensible to set as the minimum value that the quantity takes).
  *
  *   {s}`maxValue: T`
  *     : Value corresponding to the upper bound of the rightmost bin (sensible to set as the maximum value that the quantity takes).
  *
  *  ## Methods
  *    {s}`cumSum(): Array[Int]`
  *      : returns the cumulative distribution of the quantity (though only in terms of relative bin position)
  *
  *    {s}`quantile(percentile: Float): Float`
  *      : returns the value associated with a given percentile
  *
  *    {s} `getBins: Array[Int]`
  *      : accessor function for the histogram bins
  *
  *    {s} `getBin(value T):Int`
  *      : find the bin index at which {s}`value` should be placed
  *
  *    {s} `getBinCount(value: T): Int`
  *      : the number of values in the same bin as {s}`value`.
  *
  *    {s}`+= (newValue: T): Unit`
  *      : Updates the histogram by placing the given value within the correct bin.
  *
  * ```{seealso}
  * [](com.raphtory.algorithms.api.GraphState)
  * [](com.raphtory.algorithms.generic.filters.EdgeQuantileFilter)
  * [](com.raphtory.algorithms.generic.filters.VertexQuantileFilter)
  * ```
  */

abstract class Histogram[T: Numeric](noBins: Int, minValue: T, maxValue: T) {
  protected val bins: Array[Int] = Array.fill(noBins)(0)

  def cumSum(): Array[Int]
  def quantile(percentile: Float): Float
  def getBins: Array[Int] = bins
  def getBin(value: T): Int
  def getBinCount(value: T): Int
  def +=(newValue: T): Unit

}
