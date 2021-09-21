package com.raphtory.core.model.communication

import java.util.concurrent.atomic.AtomicInteger
import akka.actor.ActorRef
import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.components.RaphtoryActor.totalPartitions

import scala.collection.mutable

class VertexMessageHandler(neighbours: mutable.Map[Int,ActorRef],jobID:String) {

  val messageCount = new AtomicInteger(0)

  def sendMessage(message:VertexMessage):Unit = {
    messageCount.incrementAndGet()
    getReaderJobWorker(message.vertexId) ! message
  }

  def getCountandReset():Int = messageCount.getAndSet(0)

  def getReaderJobWorker(srcId: Long): ActorRef = {
    neighbours((srcId.abs % totalPartitions).toInt)
  }

}
