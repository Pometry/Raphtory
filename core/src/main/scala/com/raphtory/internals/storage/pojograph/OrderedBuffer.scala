package com.raphtory.internals.storage.pojograph

import com.raphtory.api.analysis.visitor.HistoricEvent
import com.raphtory.api.analysis.visitor.IndexedValue
import com.raphtory.api.analysis.visitor.SearchPoint
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.Searching.Found
import scala.collection.Searching.InsertionPoint
import scala.collection.IndexedSeqView
import scala.collection.mutable
import scala.math.Ordering.Implicits.infixOrderingOps

private[raphtory] class History[T <: IndexedValue] {
  val buffer                 = mutable.ArrayBuffer.empty[T]
  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  def first: T = buffer.head

  def last: T = buffer.last

  def clear(): Unit = buffer.clear()

  def insert(element: T): Unit =
    if (buffer.isEmpty || element > buffer.last) buffer += element
    else {
      val insertionPoint = buffer.search(element)
      insertionPoint match {
        case Found(foundIndex)              =>
          if (!(element sameValue buffer(foundIndex)))
            throw new Error(
                    s"Mismatched duplicate element: old=${buffer(foundIndex)}, new=$element"
            )
        case InsertionPoint(insertionPoint) =>
          logger.debug(s"inserting element $element out of order at index $insertionPoint of ${buffer.size}")
          buffer.insert(insertionPoint, element)
      }
    }

  def extend(elements: IterableOnce[T]): Unit = elements.iterator.foreach(e => insert(e))

  def slice(from: SearchPoint, until: SearchPoint): IndexedSeqView[T] = {
    val leftIndex  = buffer.search(from).insertionPoint
    val rightIndex = buffer.search(until, leftIndex, buffer.size).insertionPoint
    buffer.view.slice(leftIndex, rightIndex)
  }

  def before(to: SearchPoint): IndexedSeqView[T] = {
    val index = buffer.search(to) match {
      case Found(i)          => i + 1
      case InsertionPoint(i) => i
    }
    buffer.view.slice(0, index)
  }

  def after(from: SearchPoint): IndexedSeqView[T] = {
    val index = buffer.search(from).insertionPoint
    buffer.view.slice(index, buffer.size)
  }

  def closest(time: Long, index: Long): Option[T] = {
    val point = SearchPoint(time, index)
    if (buffer.isEmpty || point < buffer.head)
      None
    else
      buffer.search(point) match {
        case Found(i)          => Some(buffer(i))
        case InsertionPoint(i) => Some(buffer(i - 1))
      }
  }
}

object History {
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
