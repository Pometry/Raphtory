package com.raphtory.api.analysis.graphstate

import com.raphtory.utils.Bounded

import scala.collection.Searching.Found
import scala.collection.Searching.InsertionPoint
import scala.collection.mutable

private trait AccumulatorImplementation[-S, T] extends Accumulator[S, T] {
  def currentValue: T
  def merge(other: T): Unit
  def reset(): Unit
}

private class SimpleAccumulatorImplementation[T](
    initialValue: T,
    retainState: Boolean = false,
    op: (T, T) => T
) extends AccumulatorImplementation[T, T] {
  var currentValue: T = initialValue
  var value: T        = initialValue

  def +=(newValue: T): Unit =
    this.synchronized(this.currentValue = op(currentValue, newValue))

  def reset(): Unit = {
    if (retainState)
      value = op(value, currentValue)
    else
      value = currentValue
    currentValue = initialValue
  }

  override def merge(other: T): Unit = this += other
}

private object AccumulatorImplementation {

  def apply[T](initialValue: T, retainState: Boolean = false, op: (T, T) => T) =
    new SimpleAccumulatorImplementation[T](initialValue, retainState, op)

}

import scala.math.Numeric.Implicits.infixNumericOps

private class HistogramImplementation[T: Numeric](
    val noBins: Int,
    override val minValue: T,
    override val maxValue: T
) extends Histogram[T] {
  val bins: Array[Int] = Array.fill(noBins)(0)
  var totalCount: Int  = 0

  override def getBins: Array[Int] = bins

  override def cumSum: Array[Int] = bins.scanLeft(0)(_ + _)

  override def quantile(quantile: Float): Float = {
    val index = cumSum.search((totalCount * quantile).floor.toInt) match {
      case InsertionPoint(insertionPoint) => insertionPoint - 1
      case Found(foundIndex)              => foundIndex
    }
    minValue.toFloat + ((maxValue - minValue).toFloat / noBins) * index
  }

  override def getBin(value: T): Int =
    (noBins * (value - minValue).toFloat / (maxValue - minValue).toFloat).floor.toInt

  override def getBinCount(value: T): Int = bins(getBin(value))

  def +=(value: T): Unit =
    this.synchronized {
      val bin = getBin(value).min(noBins - 1)
      bins(bin) += 1
      totalCount += 1
    }
}

private object HistogramImplementation {

  def apply[T: Numeric](noBins: Int, minValue: T, maxValue: T) =
    new HistogramImplementation[T](noBins, minValue, maxValue)
}

private class HistogramAccumulatorImplementation[T: Numeric](
    noBins: Int,
    minValue: T,
    maxValue: T,
    retainState: Boolean
) extends AccumulatorImplementation[T, Histogram[T]] {

  var value: Histogram[T]                      = HistogramImplementation(noBins, minValue, maxValue)
  var currentValue: HistogramImplementation[T] = HistogramImplementation(noBins, minValue, maxValue)

  override def +=(value: T): Unit = currentValue += value

  override def merge(other: Histogram[T]): Unit = {
    currentValue.totalCount += other.totalCount
    val otherBins = other.getBins
    for (bindex <- currentValue.bins.indices)
      currentValue.bins(bindex) += otherBins(bindex)
  }

  override def reset(): Unit = {
    if (retainState)
      merge(value)
    val tmp = value.asInstanceOf[HistogramImplementation[T]]
    value = currentValue
    currentValue = tmp
    currentValue.totalCount = 0
    for (i <- currentValue.bins.indices)
      currentValue.bins(i) = 0
  }
}

private object HistogramAccumulatorImplementation {

  def apply[T: Numeric](noBins: Int, minValue: T, maxValue: T, retainState: Boolean) =
    new HistogramAccumulatorImplementation[T](noBins, minValue, maxValue, retainState)
}

private[raphtory] class GraphStateImplementation(override val nodeCount: Int) extends GraphState {
  private val accumulatorState = mutable.Map.empty[String, AccumulatorImplementation[Any, Any]]
  private val histogramState   = mutable.Map.empty[String, HistogramImplementation[Any]]

  def newAccumulator[T](
      name: String,
      initialValue: T,
      retainState: Boolean = false,
      op: (T, T) => T
  ): Unit =
    accumulatorState(name) = AccumulatorImplementation[T](initialValue, retainState, op)
      .asInstanceOf[AccumulatorImplementation[Any, Any]]

  def newConstant[T](
      name: String,
      value: T
  ): Unit =
    accumulatorState(name) = AccumulatorImplementation[T](value, retainState = false, (x, _) => x)
      .asInstanceOf[AccumulatorImplementation[Any, Any]]

  def newAdder[T](name: String, initialValue: T, retainState: Boolean)(implicit
      numeric: Numeric[T]
  ): Unit =
    accumulatorState(name) = AccumulatorImplementation[T](initialValue, retainState, numeric.plus)
      .asInstanceOf[AccumulatorImplementation[Any, Any]]

  def newMultiplier[T](name: String, initialValue: T, retainState: Boolean)(implicit
      numeric: Numeric[T]
  ): Unit =
    accumulatorState(name) = AccumulatorImplementation[T](initialValue, retainState, numeric.times)
      .asInstanceOf[AccumulatorImplementation[Any, Any]]

  override def newMax[T](name: String, initialValue: T, retainState: Boolean)(implicit
      numeric: Numeric[T],
      bounded: Bounded[T]
  ): Unit =
    accumulatorState(name) = AccumulatorImplementation[T](initialValue, retainState = retainState, numeric.max)
      .asInstanceOf[AccumulatorImplementation[Any, Any]]

  override def newMin[T](name: String, initialValue: T, retainState: Boolean)(implicit
      numeric: Numeric[T],
      bounded: Bounded[T]
  ): Unit =
    accumulatorState(name) = AccumulatorImplementation[T](initialValue, retainState = retainState, numeric.min)
      .asInstanceOf[AccumulatorImplementation[Any, Any]]

  override def newHistogram[T: Numeric](
      name: String,
      noBins: Int,
      minValue: T,
      maxValue: T,
      retainState: Boolean
  ): Unit =
    accumulatorState(name) = HistogramAccumulatorImplementation[T](noBins, minValue, maxValue, retainState)
      .asInstanceOf[AccumulatorImplementation[Any, Any]]

  override def newAll(name: String, retainState: Boolean): Unit =
    accumulatorState(name) = AccumulatorImplementation[Boolean](true, retainState, _ && _)
      .asInstanceOf[AccumulatorImplementation[Any, Any]]

  override def newAny(name: String, retainState: Boolean): Unit =
    accumulatorState(name) = AccumulatorImplementation[Boolean](initialValue = false, retainState, _ || _)
      .asInstanceOf[AccumulatorImplementation[Any, Any]]

  def update(graphState: GraphStateImplementation): Unit =
    graphState.accumulatorState.foreach {
      case (name, value) =>
        accumulatorState(name).merge(value.currentValue)
    }

  def rotate(): Unit                               =
    accumulatorState.foreach { case (_, accumulator) => accumulator.reset() }

  def apply[S, T](name: String): Accumulator[S, T] =
    accumulatorState(name).asInstanceOf[Accumulator[S, T]]

  override def get[S, T](name: String): Option[Accumulator[S, T]] =
    accumulatorState.get(name).asInstanceOf[Option[Accumulator[S, T]]]

  override def contains(name: String): Boolean =
    accumulatorState.contains(name)
}

private[raphtory] object GraphStateImplementation {
  def apply(nodeCount: Int) = new GraphStateImplementation(nodeCount)
  val empty                 = new GraphStateImplementation(0)
}
