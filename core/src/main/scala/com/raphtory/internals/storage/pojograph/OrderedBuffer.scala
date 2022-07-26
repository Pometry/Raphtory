package com.raphtory.internals.storage.pojograph

import com.raphtory.api.analysis.visitor.HistoricEvent
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.Searching.Found
import scala.collection.Searching.InsertionPoint
import scala.collection.mutable
import scala.math.Ordering.Implicits.infixOrderingOps

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
