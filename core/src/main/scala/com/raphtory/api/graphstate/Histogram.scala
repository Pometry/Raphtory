package com.raphtory.api.graphstate

/**
  * Public interface for the Histograms API within [[GraphState]].
  *
  *  A Histogram maintains the distribution of a graph quantity in bins between the minimum and maximum value that
  *  this quantity takes.
  *
  *  @tparam T Numeric value type of the histogram
  *
  * @see
  * [[GraphState]],
  * <a href="../../../../../_autodoc/com/raphtory/algorithms/generic/filters/EdgeQuantileFilter.html"
  *  class="extype"
  *  title="com.raphtory.algorithms.generic.filters.EdgeQuantileFilter"
  * >EdgeQuantileFilter</a>,
  * <a href="../../../../../_autodoc/com/raphtory/algorithms/generic/filters/VertexQuantileFilter.html"
  *  class="extype"
  *  title="com.raphtory.algorithms.generic.filters.VertexQuantileFilter"
  * >VertexQuantileFilter</a>
  */

abstract class Histogram[T: Numeric] {

  /** Minimum data value for the histogram */
  val minValue: T

  /** Maximum data value for the histogram */
  val maxValue: T

  /** Return the total population size */
  def totalCount: Int

  /** Compute the cumulative density function of the property histogram unnormalised (in raw counts so adds to `totalCount`) */
  def cumSum: Array[Int]

  /** Cumulative density function normalised */
  def cdf: Array[Float] = pdf.scanLeft(0.0f)(_ + _)

  /** Probability density function */
  def pdf: Array[Float] = getBins.map(_.toFloat / totalCount)

  /** Compute the value associated with a given quantile
    *  @param quantile  quantile to find. For example, 0.5 would correspond to the median value.
    */
  def quantile(quantile: Float): Float

  /** Getter function for obtaining the bins counts of the histogram.
    */
  def getBins: Array[Int]

  /** Get the bin index associated with a given value.
    *
    * @param value Input value
    *
    * @return Bin index
    */
  def getBin(value: T): Int

  /** Find the population of a bin associated with a given value.
    *
    * @param value Input value
    *
    * @return Count of elements in corresponding bin
    */
  def getBinCount(value: T): Int
}
