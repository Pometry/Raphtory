package com.raphtory.api.analysis.graphstate

import com.raphtory.utils.Bounded

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import scala.collection.Searching.Found
import scala.collection.Searching.InsertionPoint
import scala.collection.MapView
import scala.collection.mutable
import scala.jdk.CollectionConverters.ConcurrentMapHasAsScala
import scala.jdk.FunctionConverters._
import scala.math.Numeric.Implicits.infixNumericOps

sealed trait AccumulatorImplementation[@specialized(Int, Long, Double) -S, T] extends Accumulator[S, T] {
  def currentValue: T
  def merge(other: T): Unit
  def reset(): Unit
}

private object AccumulatorImplementation {

  def apply[@specialized(Int, Long, Double) T](initialValue: T, retainState: Boolean = false, op: (T, T) => T) =
    new ConcurrentAccumulatorImpl[T](initialValue, retainState, op)

}

private class ConcurrentAccumulatorImpl[@specialized(Int, Long, Double) T](
    initialValue: T,
    retainState: Boolean = false,
    op: (T, T) => T
) extends AccumulatorImplementation[T, T] {
  private val currentValueA    = new AtomicReference[T](initialValue)
  private val valueA           = new AtomicReference[T](initialValue)
  override def currentValue: T = currentValueA.get()

  override def merge(other: T): Unit = this.+=(other)

  override def reset(): Unit =
    this.synchronized {
      if (retainState)
        valueA.accumulateAndGet(currentValue, op.asJava)
      else
        valueA.set(currentValue)
      currentValueA.set(initialValue)
    }

  /** Get last accumulated value */
  override def value: T = valueA.get()

  /** Add new value to accumulator
    *
    * @param newValue Value to add
    */
  override def +=(newValue: T): Unit =
    this.currentValueA.accumulateAndGet(newValue, op.asJava)
}

private class IntAccumulatorImpl(initialValue: Int, retainState: Boolean = false, op: (Int, Int) => Int)
        extends AccumulatorImplementation[Int, Int] {
  private val currentValueA: AtomicInteger = new AtomicInteger(initialValue)
  private val valueA: AtomicInteger        = new AtomicInteger(initialValue)
  override def currentValue: Int           = currentValueA.get()

  override def merge(other: Int): Unit = this += other

  override def reset(): Unit = {
    if (retainState)
      valueA.accumulateAndGet(currentValue, op.asJava)
    else
      valueA.set(currentValue)
    currentValueA.set(initialValue)
  }

  /** Get last accumulated value */
  override def value: Int = valueA.get()

  /** Add new value to accumulator
    *
    * @param newValue Value to add
    */
  override def +=(newValue: Int): Unit =
    currentValueA.accumulateAndGet(newValue, op.asJava)
}

private class CounterImplementation[T]() extends Counter[T] {
  var totalCount: Int = 0
  var counts          = new java.util.concurrent.ConcurrentHashMap[T, Int]()

  override def getCounts: MapView[T, Int] = counts.asScala.view
  override def largest: (T, Int)          = getCounts.maxBy(_._2)

  override def largest(k: Int): Iterable[(T, Int)] = {
    val ord = Ordering.by[(T, Int), Int](_._2).reverse
    counts.asScala.foldLeft(new mutable.PriorityQueue[(T, Int)]()(ord)) { (q, c) =>
      q.enqueue(c)
      if (q.size > k) {
        q.dequeue()
        q
      }
      else q
    }
  }

  def +=(newValue: T, c: Int): Unit =
    counts.compute(
            newValue,
            {
              case (t, c1) => c + c1
              case _       => c
            }
    )

}

private object CounterImplementation {
  def apply[T]() = new CounterImplementation[T]()
}

private class CounterAccumulatorImplementation[T](retainState: Boolean)
        extends AccumulatorImplementation[(T, Int), Counter[T]] {
  var value: Counter[T]                      = CounterImplementation()
  var currentValue: CounterImplementation[T] = CounterImplementation()

  override def merge(other: Counter[T]): Unit = {
    currentValue.totalCount += other.totalCount
    other.getCounts.foreach{case (tag, count) => currentValue.counts.merge(tag, count, (c1, c2) => c1 + c2)}
  }

  override def reset(): Unit = {
    if (retainState)
      merge(value)
    val tmp = value.asInstanceOf[CounterImplementation[T]]
    value = currentValue
    currentValue = tmp
    currentValue.totalCount = 0
  }

  /** Add new value to accumulator
    *
    * @param newValue Value to add
    */
  override def +=(newValue: (T, Int)): Unit = currentValue +=(newValue._1, newValue._2)
}

private object CounterAccumulatorImplementation {
  def apply[T](retainState: Boolean) = new CounterAccumulatorImplementation[T](retainState)
}

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
