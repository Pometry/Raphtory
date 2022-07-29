package com.raphtory.internals.storage.pojograph

import com.raphtory.api.analysis.visitor.HistoricEvent
import com.raphtory.api.analysis.visitor.IndexedValue
import com.raphtory.api.analysis.visitor.TimePoint
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.Searching.Found
import scala.collection.Searching.InsertionPoint
import scala.collection.mutable
import scala.math.Ordering.Implicits.infixOrderingOps

trait HistoryOps[T <: IndexedValue] extends IndexedSeq[T] {
  def first: T
  def last: T
  def slice(from: IndexedValue, until: IndexedValue): HistoryView[T]
  def before(to: IndexedValue): HistoryView[T]
  def after(from: IndexedValue): HistoryView[T]
  def closest(time: IndexedValue): Option[T]
  def iterateUniqueTimes: Iterator[T] = new IterateUniqueTimePoints[T](this)
}

private[raphtory] class IterateUniqueTimePoints[T <: IndexedValue](underlying: HistoryOps[T]) extends Iterator[T] {
  private var index: Int     = 0
  private var lastTime: Long = Long.MinValue

  override def hasNext: Boolean = {
    while (index < underlying.length && underlying(index).time == lastTime)
      index += 1
    index < underlying.length
  }

  override def next(): T =
    if (hasNext) {
      val value = underlying(index)
      lastTime = value.time
      value
    }
    else
      throw new NoSuchElementException
}

private[raphtory] class HistoryView[T <: IndexedValue](startIndex: Int, endIndex: Int, buffer: mutable.ArrayBuffer[T])
        extends HistoryOps[T] {

  def apply(i: Int): T =
    buffer(startIndex + i)

  def length: Int = endIndex - startIndex

  def slice(from: IndexedValue, until: IndexedValue): HistoryView[T] = {
    val leftIndex  = buffer.search(from, startIndex, endIndex).insertionPoint
    val rightIndex = buffer.search(until, leftIndex, endIndex).insertionPoint
    new HistoryView[T](leftIndex, rightIndex, buffer)
  }

  def before(to: IndexedValue): HistoryView[T] = {
    val index = buffer.search(to, startIndex, endIndex) match {
      case Found(i)          => i + 1
      case InsertionPoint(i) => i
    }
    new HistoryView[T](startIndex, index, buffer)
  }

  def after(from: IndexedValue): HistoryView[T] = {
    val index = buffer.search(from, startIndex, endIndex).insertionPoint
    new HistoryView[T](index, endIndex, buffer)
  }

  def closest(time: IndexedValue): Option[T] =
    if (isEmpty || time < head)
      None
    else
      buffer.search(time, startIndex, endIndex) match {
        case Found(i)          => Some(buffer(i))
        case InsertionPoint(i) => Some(buffer(i - 1))
      }

  override def first: T = head
}

private[raphtory] class History[T <: IndexedValue] extends HistoryOps[T] {
  val buffer = mutable.ArrayBuffer.empty[T]

  def apply(i: Int): T = buffer(i)

  def length: Int = buffer.length

  def first: T = buffer.head

  def clear(): Unit = buffer.clear()

  def insert(element: T): Unit =
    if (buffer.isEmpty || element > buffer.last) {
      buffer += element
      History.logger.trace(s"Inserting element $element at the end.")
    }
    else {
      val insertionPoint = buffer.search(element)
      insertionPoint match {
        case Found(foundIndex)              =>
          if (!(element sameValue buffer(foundIndex)))
            throw new Error(
                    s"Mismatched duplicate element: old=${buffer(foundIndex)}, new=$element"
            )
        case InsertionPoint(insertionPoint) =>
          History.logger.debug(s"inserting element $element out of order at index $insertionPoint of ${buffer.size}")
          buffer.insert(insertionPoint, element)
      }
    }

  def extend(elements: IterableOnce[T]): Unit = elements.iterator.foreach(e => insert(e))

  def slice(from: IndexedValue, until: IndexedValue): HistoryView[T] = {
    val leftIndex  = buffer.search(from).insertionPoint
    val rightIndex = buffer.search(until, leftIndex, buffer.size).insertionPoint
    new HistoryView[T](leftIndex, rightIndex, buffer)
  }

  def before(to: IndexedValue): HistoryView[T] = {
    val index = buffer.search(to) match {
      case Found(i)          => i + 1
      case InsertionPoint(i) => i
    }
    new HistoryView[T](0, index, buffer)
  }

  def after(from: IndexedValue): HistoryView[T] = {
    val index = buffer.search(from).insertionPoint
    new HistoryView[T](index, buffer.size, buffer)
  }

  def closest(time: IndexedValue): Option[T] =
    if (buffer.isEmpty || time < buffer.head)
      None
    else
      buffer.search(time) match {
        case Found(i)          => Some(buffer(i))
        case InsertionPoint(i) => Some(buffer(i - 1))
      }
}

object History {
  val logger: Logger             = Logger(LoggerFactory.getLogger(this.getClass))
  def apply[T <: IndexedValue]() = new History[T]
}

private[raphtory] object OrderedBuffer {
  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  implicit class OrderedAppend[T: Ordering](buffer: mutable.Buffer[T]) {

    def sortedAppend(element: T): Unit =
      if (buffer.isEmpty || element > buffer.last)
        buffer += element
      else {
        val insertionPoint = buffer.search(element)
        insertionPoint match {
          case Found(foundIndex)              =>
            if (!(element == buffer(foundIndex)))
              throw new Error(
                      s"Mismatched duplicate element: old=${buffer(foundIndex)}, new=$element"
              )
          case InsertionPoint(insertionPoint) =>
            logger.trace("inserting element out of order")
            buffer.insert(insertionPoint, element)
        }
      }

    def sortedExtend(elements: IterableOnce[T]): Unit =
      elements.iterator.foreach(sortedAppend)
  }
}
