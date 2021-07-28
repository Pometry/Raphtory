package com.raphtory.core.model.communication

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorRef
import akka.cluster.pubsub.DistributedPubSubMediator

import scala.collection.mutable

class VertexMessageHandler(neighbours: mutable.Map[(Int,Int),ActorRef],managerCount:Int,jobID:String) {

  val messageCount = new AtomicInteger(0)

  def sendMessage(message:VertexMessage):Unit = {
    messageCount.incrementAndGet()
    getReaderJobWorker(message.vertexId, managerCount) ! message
  }

  def getCountandReset():Int = messageCount.getAndSet(0)

  def getReaderJobWorker(srcId: Long, managerCount: Int): ActorRef = {
    val mod     = srcId.abs % (managerCount * 10)
    val manager = (mod / 10).toInt
    val worker  = (mod % 10).toInt
    neighbours((manager,worker))
  }

}
