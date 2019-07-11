package com.raphtory.core.model.communication

import scala.collection.mutable

class VertexMutliQueue {

  val evenMessageQueueMap = mutable.HashMap[String,mutable.ArrayStack[VertexMessage]]()
  val oddMessageQueueMap = mutable.HashMap[String,mutable.ArrayStack[VertexMessage]]()

  def getMessageQueue(jobID:String,superStep:Int):mutable.ArrayStack[VertexMessage] = {
    val queueMap = if (superStep % 2 ==0) evenMessageQueueMap else oddMessageQueueMap
    queueMap.getOrElseUpdate(jobID,mutable.ArrayStack[VertexMessage]())
  }

  def receiveMessage(handler: MessageHandler) = getMessageQueue(handler.jobID,handler.superStep).push(handler.message)


}
