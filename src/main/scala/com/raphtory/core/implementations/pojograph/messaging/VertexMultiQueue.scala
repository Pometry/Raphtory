package com.raphtory.core.implementations.pojograph.messaging

import scala.collection.mutable.ListBuffer


final class VertexMultiQueue {
  private val evenMessageQueue: ListBuffer[Any] = ListBuffer.empty
  private val oddMessageQueue: ListBuffer[Any]  = ListBuffer.empty

  def getMessageQueue(superStep: Int): List[Any] =
    if (superStep % 2 == 0) {
      evenMessageQueue.toList
    } else
      oddMessageQueue.toList

  def clearQueue(superStep: Int): Unit = if (superStep % 2 == 0) evenMessageQueue.clear() else oddMessageQueue.clear()

  def receiveMessage(superStep: Int, data: Any):Unit = {

    if ((superStep) % 2 == 0){
      evenMessageQueue += data
    } else {
      oddMessageQueue += data
    }
  }

}
