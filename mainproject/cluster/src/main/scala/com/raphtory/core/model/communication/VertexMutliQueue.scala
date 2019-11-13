package com.raphtory.core.model.communication

import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap

class VertexMutliQueue {

  val evenMessageQueueMap = ParTrieMap[String,mutable.ArrayBuffer[Any]]()
  val oddMessageQueueMap = ParTrieMap[String,mutable.ArrayBuffer[Any]]()

  def getMessageQueue(jobID:String,superStep:Int):mutable.ArrayBuffer[Any] = {
    val queueMap = if (superStep % 2 ==0) evenMessageQueueMap else oddMessageQueueMap
    queueMap.get(jobID) match {
      case Some(stack) => stack
      case None =>  {
        val newStack = mutable.ArrayBuffer[Any]()
        queueMap(jobID) = newStack
        newStack
      }
    }
  }

  def clearQueue(jobID:String,superStep:Int)= {
    val queueMap = if (superStep % 2 ==0) evenMessageQueueMap else oddMessageQueueMap
    queueMap(jobID) = mutable.ArrayBuffer[Any]()
  }

  def clearQueues(jobID:String) = {
    evenMessageQueueMap.get(jobID) match {
      case Some(q) => evenMessageQueueMap(jobID) = mutable.ArrayBuffer[Any]()
      case None =>
    }
    oddMessageQueueMap.get(jobID) match {
      case Some(q) => oddMessageQueueMap(jobID) = mutable.ArrayBuffer[Any]()
      case None =>
    }
  }


  def receiveMessage(jobID:String,superStep:Int,data:Any) = getMessageQueue(jobID,superStep+1) +=(data)


}
