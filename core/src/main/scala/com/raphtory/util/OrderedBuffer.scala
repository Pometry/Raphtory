package com.raphtory.util

import com.raphtory.api.visitor.HistoricEvent
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.Ordering.Implicits._
import scala.collection.Searching.Found
import scala.collection.Searching.InsertionPoint

object OrderedBuffer {
  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  implicit object TupleByFirstOrdering extends Ordering[(Long, Any)] {
    def compare(a: (Long, Any), b: (Long, Any)): Int = a._1 compare b._1
  }

  implicit object HistoricEventOrdering extends Ordering[HistoricEvent] {
    def compare(a: HistoricEvent, b: HistoricEvent): Int = a.time compare b.time
  }

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
