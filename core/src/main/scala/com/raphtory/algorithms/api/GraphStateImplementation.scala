package com.raphtory.algorithms.api

import scala.collection.Searching.Found
import scala.collection.Searching.InsertionPoint
import scala.collection.mutable

/**
  * @note DoNotDocument
  */
private class AccumulatorImplementation[T](
    initialValue: T,
    retainState: Boolean = false,
    op: (T, T) => T
) extends Accumulator[T] {
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
}

/**
  * @note DoNotDocument
  */
private object AccumulatorImplementation {

  def apply[T](initialValue: T, retainState: Boolean = false, op: (T, T) => T) =
    new AccumulatorImplementation[T](initialValue, retainState, op)

}

import scala.math.Numeric.Implicits.infixNumericOps

private class HistogramImplementation[T: Numeric](
    noBins: Int,
    minValue: T,
    maxValue: T
) extends Histogram[T](noBins, minValue, maxValue) {

  private var totalIncrements = 0

  override def +=(value: T): Unit = {
    val bin = getBin(value).min(noBins-1)
    bins(bin) = bins(bin) + 1
    totalIncrements += 1
  }

  override def getBin(value: T): Int =
    (noBins * (value - minValue).toFloat / (maxValue - minValue).toFloat).floor.toInt

  override def getBinCount(value: T): Int = bins(getBin(value))

  def mergeBins(partialBin: Array[Int]): Unit = bins.zip(partialBin).map { case (a, b) => a + b }

  override def cumSum(): Array[Int] = bins.scanLeft(0)(_ + _)

  override def quantile(percentile: Float): Float = {
    val index = cumSum.search((totalIncrements * percentile).floor.toInt) match {
      case InsertionPoint(insertionPoint) => insertionPoint - 1
      case Found(foundIndex)              => foundIndex
    }
    minValue.toFloat + ((maxValue - minValue).toFloat / noBins) * index
  }

}

/**
  * @DoNotDocument
  */
private object HistogramImplementation {

  def apply[T: Numeric](noBins: Int, minValue: T, maxValue: T) =
    new HistogramImplementation[T](noBins, minValue, maxValue)
}

class Bounded[T](min: T, max: T) {
  def MIN: T = min
  def MAX: T = max
}

/**
  * @note DoNotDocument
  */
object Bounded {
  def apply[T](min: T, max: T) = new Bounded[T](min, max)

  implicit val intBounds: Bounded[Int]   = Bounded(Int.MinValue, Int.MaxValue)
  implicit val longBounds: Bounded[Long] = Bounded(Long.MinValue, Long.MaxValue)

  implicit val doubleBounds: Bounded[Double] =
    Bounded(Double.NegativeInfinity, Double.PositiveInfinity)

  implicit val floatBounds: Bounded[Float] =
    Bounded(Float.NegativeInfinity, Float.PositiveInfinity)
}

/**
  * @note DoNotDocument
  */
class GraphStateImplementation extends GraphState {
  private val accumulatorState = mutable.Map.empty[String, AccumulatorImplementation[Any]]
  private val histogramState   = mutable.Map.empty[String, HistogramImplementation[Any]]

  /** @inheritdoc */
  def newAccumulator[T](
      name: String,
      initialValue: T,
      retainState: Boolean = false,
      op: (T, T) => T
  ): Unit =
    accumulatorState(name) = AccumulatorImplementation[T](initialValue, retainState, op)
      .asInstanceOf[AccumulatorImplementation[Any]]

  def newConstant[T](
      name: String,
      value: T
  ): Unit =
    accumulatorState(name) = AccumulatorImplementation[T](value, retainState = false, (x, _) => x)
      .asInstanceOf[AccumulatorImplementation[Any]]

  def newAdder[T](name: String)(implicit numeric: Numeric[T]): Unit =
    accumulatorState(name) =
      AccumulatorImplementation[T](initialValue = numeric.zero, retainState = false, numeric.plus)
        .asInstanceOf[AccumulatorImplementation[Any]]

  def newAdder[T](name: String, retainState: Boolean)(implicit numeric: Numeric[T]): Unit =
    accumulatorState(name) =
      AccumulatorImplementation[T](initialValue = numeric.zero, retainState, numeric.plus)
        .asInstanceOf[AccumulatorImplementation[Any]]

  def newAdder[T](name: String, initialValue: T)(implicit numeric: Numeric[T]): Unit =
    accumulatorState(name) =
      AccumulatorImplementation[T](initialValue, retainState = false, numeric.plus)
        .asInstanceOf[AccumulatorImplementation[Any]]

  def newAdder[T](name: String, initialValue: T, retainState: Boolean)(implicit
      numeric: Numeric[T]
  ): Unit =
    accumulatorState(name) = AccumulatorImplementation[T](initialValue, retainState, numeric.plus)
      .asInstanceOf[AccumulatorImplementation[Any]]

  /** @inheritdoc */
  override def newMultiplier[T](name: String)(implicit numeric: Numeric[T]): Unit =
    accumulatorState(name) =
      AccumulatorImplementation[T](numeric.one, retainState = false, op = numeric.times)
        .asInstanceOf[AccumulatorImplementation[Any]]

  /** @inheritdoc */
  override def newMultiplier[T](name: String, initialValue: T)(implicit numeric: Numeric[T]): Unit =
    accumulatorState(name) =
      AccumulatorImplementation[T](initialValue, retainState = false, op = numeric.times)
        .asInstanceOf[AccumulatorImplementation[Any]]

  /** @inheritdoc */
  override def newMultiplier[T](name: String, retainState: Boolean)(implicit
      numeric: Numeric[T]
  ): Unit =
    accumulatorState(name) = AccumulatorImplementation[T](numeric.one, retainState, numeric.times)
      .asInstanceOf[AccumulatorImplementation[Any]]

  /** Create a new accumulator that multiplies values */
  override def newMultiplier[T](name: String, initialValue: T, retainState: Boolean)(implicit
      numeric: Numeric[T]
  ): Unit =
    accumulatorState(name) = AccumulatorImplementation[T](initialValue, retainState, numeric.times)
      .asInstanceOf[AccumulatorImplementation[Any]]

  /** @inheritdoc */
  override def newMax[T](name: String)(implicit numeric: Numeric[T], bounded: Bounded[T]): Unit =
    accumulatorState(name) =
      AccumulatorImplementation[T](bounded.MIN, retainState = false, numeric.max)
        .asInstanceOf[AccumulatorImplementation[Any]]

  /** @inheritdoc */
  override def newMax[T](name: String, retainState: Boolean)(implicit
      numeric: Numeric[T],
      bounded: Bounded[T]
  ): Unit =
    accumulatorState(name) =
      AccumulatorImplementation[T](bounded.MIN, retainState = retainState, numeric.max)
        .asInstanceOf[AccumulatorImplementation[Any]]

  /** @inheritdoc */
  override def newMax[T](name: String, initialValue: T)(implicit
      numeric: Numeric[T],
      bounded: Bounded[T]
  ): Unit =
    accumulatorState(name) =
      AccumulatorImplementation[T](initialValue, retainState = false, numeric.max)
        .asInstanceOf[AccumulatorImplementation[Any]]

  /** @inheritdoc */
  override def newMax[T](name: String, initialValue: T, retainState: Boolean)(implicit
      numeric: Numeric[T],
      bounded: Bounded[T]
  ): Unit =
    accumulatorState(name) =
      AccumulatorImplementation[T](initialValue, retainState = retainState, numeric.max)
        .asInstanceOf[AccumulatorImplementation[Any]]

  /** @inheritdoc */
  override def newMin[T](name: String)(implicit numeric: Numeric[T], bounded: Bounded[T]): Unit =
    accumulatorState(name) =
      AccumulatorImplementation[T](bounded.MAX, retainState = false, numeric.min)
        .asInstanceOf[AccumulatorImplementation[Any]]

  /** @inheritdoc */
  override def newMin[T](name: String, retainState: Boolean)(implicit
      numeric: Numeric[T],
      bounded: Bounded[T]
  ): Unit =
    accumulatorState(name) =
      AccumulatorImplementation[T](bounded.MAX, retainState = retainState, numeric.min)
        .asInstanceOf[AccumulatorImplementation[Any]]

  /** @inheritdoc */
  override def newMin[T](name: String, initialValue: T)(implicit
      numeric: Numeric[T],
      bounded: Bounded[T]
  ): Unit =
    accumulatorState(name) =
      AccumulatorImplementation[T](initialValue, retainState = false, numeric.min)
        .asInstanceOf[AccumulatorImplementation[Any]]

  /** @inheritdoc */
  override def newMin[T](name: String, initialValue: T, retainState: Boolean)(implicit
      numeric: Numeric[T],
      bounded: Bounded[T]
  ): Unit =
    accumulatorState(name) =
      AccumulatorImplementation[T](initialValue, retainState = retainState, numeric.min)
        .asInstanceOf[AccumulatorImplementation[Any]]

  override def newHistogram[T: Numeric](name: String, noBins: Int, minValue: T, maxValue: T): Unit =
    histogramState(name) = HistogramImplementation(noBins: Int, minValue: T, maxValue: T)
      .asInstanceOf[HistogramImplementation[Any]]

  override def newAll(name: String, retainState: Boolean): Unit =
    accumulatorState(name) = AccumulatorImplementation[Boolean](true, retainState, _ && _)
      .asInstanceOf[AccumulatorImplementation[Any]]

  /** @inheritdoc */
  override def newAny(name: String, retainState: Boolean): Unit =
    accumulatorState(name) =
      AccumulatorImplementation[Boolean](initialValue = false, retainState, _ || _)
        .asInstanceOf[AccumulatorImplementation[Any]]

  def update(graphState: GraphStateImplementation): Unit = {
    graphState.accumulatorState.foreach {
      case (name, value) =>
        accumulatorState(name) += value.currentValue
    }
    graphState.histogramState.foreach {
      case (name, partitalHist) =>
        histogramState(name).mergeBins(partitalHist.getBins)
    }
  }

  def rotate(): Unit                         =
    accumulatorState.foreach { case (_, accumulator) => accumulator.reset() }

  def apply[T](name: String): Accumulator[T] =
    accumulatorState(name).asInstanceOf[Accumulator[T]]

  /** @inheritdoc */
  override def get[T](name: String): Option[Accumulator[T]] =
    accumulatorState.get(name).asInstanceOf[Option[Accumulator[T]]]

  /** @inheritdoc */
  override def contains(name: String): Boolean =
    accumulatorState.contains(name)

  def getHistogram[T: Numeric](name: String): Option[Histogram[T]] =
    histogramState.get(name).asInstanceOf[Option[Histogram[T]]]

  def containsHistogram(name: String): Boolean = histogramState.contains(name)

}

object GraphStateImplementation {
  def apply() = new GraphStateImplementation
  val empty   = new GraphStateImplementation
}
