package com.raphtory.internals.storage.pojograph.messaging

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import VertexMultiQueue.logger

import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters._

final private[raphtory] class VertexMultiQueue {

  private val evenMessageQueue = new ConcurrentLinkedQueue[Any]()
  private val oddMessageQueue  = new ConcurrentLinkedQueue[Any]()

  def getMessageQueue(superStep: Int): Vector[Any] =
    if (superStep % 2 == 0) {
      logger.debug(s"retreived even message queue at step $superStep")
      evenMessageQueue.asScala.toVector
    }
    else {
      logger.debug(s"retreived odd message queue at step $superStep")
      oddMessageQueue.asScala.toVector
    }

  def clearQueue(superStep: Int): Unit =
    if (superStep % 2 == 0) {
      logger.debug(s"Clearing even message queue at super step '$superStep'.")
      evenMessageQueue.clear()
    }
    else {
      logger.debug(s"Clearing odd message queue at super step '$superStep'.")
      oddMessageQueue.clear()
    }

  def clearAll(): Unit = {
    evenMessageQueue.clear()
    oddMessageQueue.clear()
    logger.debug(s"Clearing both message queues")
  }

  def receiveMessage(superStep: Int, data: Any): Unit =
    if (superStep % 2 == 0) {
      evenMessageQueue.add(data)
      logger.debug(s"data $data added to even queue at step $superStep")
    }
    else {
      logger.debug(s"data $data added to odd queue at step $superStep")
      oddMessageQueue.add(data)
    }

}

object VertexMultiQueue {
  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))
}
