package com.raphtory.internals.storage.pojograph.messaging

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

final private[raphtory] class VertexMultiQueue {
  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  private val evenMessageQueue: ArrayBuffer[Any] = ArrayBuffer.empty
  private val oddMessageQueue: ArrayBuffer[Any]  = ArrayBuffer.empty

  def getMessageQueue(superStep: Int): List[Any] =
    if (superStep % 2 == 0)
      evenMessageQueue.toList
    else
      oddMessageQueue.toList

  def clearQueue(superStep: Int): Unit =
    if (superStep % 2 == 0) {
      logger.trace(s"Clearing even message queue at super step '$superStep'.")
      evenMessageQueue.clear()
    }
    else {
      logger.trace(s"Clearing odd message queue at super step '$superStep'.")
      oddMessageQueue.clear()
    }

  def clearAll(): Unit = {
    evenMessageQueue.clear()
    oddMessageQueue.clear()
    logger.trace(s"Clearing both message queues")
  }

  def receiveMessage(superStep: Int, data: Any): Unit =
    if (superStep % 2 == 0)
      evenMessageQueue.synchronized(evenMessageQueue += data)
    else
      oddMessageQueue.synchronized(oddMessageQueue += data)

}
