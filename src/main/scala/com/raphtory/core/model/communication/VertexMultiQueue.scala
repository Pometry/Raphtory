package com.raphtory.core.model.communication

import scala.collection.mutable.{ArrayBuffer, MutableList}

final class VertexMultiQueue {
  private val evenMessageQueue: ArrayBuffer[Any] = ArrayBuffer.empty
  private val oddMessageQueue: ArrayBuffer[Any]  = ArrayBuffer.empty

  def getMessageQueue(superStep: Int): ArrayBuffer[Any] =
    if (superStep % 2 == 0) evenMessageQueue else oddMessageQueue

  def clearQueue(superStep: Int): Unit =
    getMessageQueue(superStep).clear()

  def clearQueues(): Unit = {
    evenMessageQueue.clear()
    oddMessageQueue.clear()
  }

  def receiveMessage(superStep: Int, data: Any) = getMessageQueue(superStep + 1) += data
}
