package com.raphtory.core.model.communication

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorRef
import akka.cluster.pubsub.DistributedPubSubMediator

class VertexMessageHandler(mediator: ActorRef,managerCount:Int) {

  val messageCount = new AtomicInteger(0)

  def sendMessage(message:VertexMessage):Unit = {
    messageCount.incrementAndGet()
    mediator ! new DistributedPubSubMediator.Send(getReader(message.vertexId, managerCount), message)
  }

  def getCountandReset():Int = messageCount.getAndSet(0)

  def getReader(srcId: Long, managerCount: Int): String = {
    val mod     = srcId.abs % (managerCount * 10)
    val manager = mod / 10
    val worker  = mod % 10
    s"/user/Manager_${manager}_reader_$worker"
  }

}
