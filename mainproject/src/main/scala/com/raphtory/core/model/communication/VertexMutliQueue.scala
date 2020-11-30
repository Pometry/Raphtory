package com.raphtory.core.model.communication

import com.raphtory.core.actors.PartitionManager.Workers.ViewJob

import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap

class VertexMutliQueue {

  val evenMessageQueueMap = ParTrieMap[ViewJob, mutable.ArrayBuffer[Any]]()
  val oddMessageQueueMap  = ParTrieMap[ViewJob, mutable.ArrayBuffer[Any]]()

  def getMessageQueue(jobID: ViewJob, superStep: Int): mutable.ArrayBuffer[Any] = {
    val queueMap = if (superStep % 2 == 0) evenMessageQueueMap else oddMessageQueueMap
    queueMap.get(jobID) match {
      case Some(stack) => stack
      case None =>
        val newStack = mutable.ArrayBuffer[Any]()
        queueMap(jobID) = newStack
        newStack
    }
  }

  def clearQueue(jobID: ViewJob, superStep: Int) = {
    val queueMap = if (superStep % 2 == 0) evenMessageQueueMap else oddMessageQueueMap
    queueMap(jobID) = mutable.ArrayBuffer[Any]()
  }

  def clearQueues(jobID: ViewJob) = {
    evenMessageQueueMap.get(jobID) match {
      case Some(q) => evenMessageQueueMap(jobID) = mutable.ArrayBuffer[Any]()
      case None    =>
    }
    oddMessageQueueMap.get(jobID) match {
      case Some(q) => oddMessageQueueMap(jobID) = mutable.ArrayBuffer[Any]()
      case None    =>
    }
  }

  def receiveMessage(jobID: ViewJob, superStep: Int, data: Any) = getMessageQueue(jobID, superStep + 1) += (data)

}
