package com.raphtory.core.model.communication

import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap

class VertexMutliQueue {

  val evenMessageQueueMap = ParTrieMap[String,mutable.ArrayStack[VertexMessage]]()
  val oddMessageQueueMap = ParTrieMap[String,mutable.ArrayStack[VertexMessage]]()

  def getMessageQueue(jobID:String,superStep:Int):mutable.ArrayStack[VertexMessage] = {
    val queueMap = if (superStep % 2 ==0) evenMessageQueueMap else oddMessageQueueMap
    queueMap.get(jobID) match {
      case Some(stack) => stack
      case None =>  {
        val newStack = mutable.ArrayStack[VertexMessage]()
        queueMap(jobID) = newStack
        newStack
      }
    }
  }

  def receiveMessage(handler: MessageHandler) = getMessageQueue(handler.jobID,handler.superStep).push(handler.message)


}
