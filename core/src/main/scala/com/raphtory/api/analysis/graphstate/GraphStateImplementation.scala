package com.raphtory.api.analysis.graphstate

import com.raphtory.utils.Bounded

import scala.collection.mutable

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

  def newAdder[@specialized(Long, Double) T](name: String, initialValue: T, retainState: Boolean)(implicit
      numeric: Numeric[T]
  ): Unit =
    accumulatorState(name) = AccumulatorImplementation[T](initialValue, retainState, numeric.plus)
      .asInstanceOf[AccumulatorImplementation[Any, Any]]

  override def newIntAdder(name: String, initalValue: Int, retainState: Boolean): Unit =
    accumulatorState(name) = new IntAccumulatorImpl(initialValue = initalValue, retainState = retainState, op = _ + _)
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

  override def newCounter[T](name: String, retainState: Boolean): Unit =
    accumulatorState(name) = CounterAccumulatorImplementation[T](retainState)
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

  override def newConcurrentAccumulator[T](name: String, initialValue: T, retainState: Boolean, op: (T, T) => T): Unit =
    accumulatorState(name) =
      new ConcurrentAccumulatorImpl[T](initialValue, retainState, op).asInstanceOf[AccumulatorImplementation[Any, Any]]
}

private[raphtory] object GraphStateImplementation {
  def apply(nodeCount: Int) = new GraphStateImplementation(nodeCount)
  val empty                 = new GraphStateImplementation(0)
}
