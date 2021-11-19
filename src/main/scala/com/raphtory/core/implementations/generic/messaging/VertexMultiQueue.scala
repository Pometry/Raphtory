package com.raphtory.core.implementations.generic.messaging

import scala.collection.mutable.{ArrayBuffer, ListBuffer}


final class VertexMultiQueue {
  private val evenMessageQueue: ArrayBuffer[Any] = ArrayBuffer.empty
  private val oddMessageQueue: ArrayBuffer[Any]  = ArrayBuffer.empty

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
