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

  def clearQueues(jobID:String) = {
    evenMessageQueueMap.get(jobID) match {
      case Some(q) => evenMessageQueueMap(jobID) = mutable.ArrayStack[VertexMessage]()
      case None =>
    }
    oddMessageQueueMap.get(jobID) match {
      case Some(q) => oddMessageQueueMap(jobID) = mutable.ArrayStack[VertexMessage]()
      case None =>
    }
  }

  def receiveMessage(handler: MessageHandler) = getMessageQueue(handler.jobID,handler.superStep+1).push(handler.message)


}
