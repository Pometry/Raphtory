package com.raphtory.core.implementations.generic.messaging

import java.util.concurrent.atomic.AtomicInteger
import akka.actor.ActorRef
import com.raphtory.core.components.akkamanagement.RaphtoryActor
import com.raphtory.core.components.akkamanagement.RaphtoryActor._
import com.raphtory.core.model.graph.{GraphUpdateEffect, TrackedGraphEffect, VertexMessage, VertexMessageBatch}

import scala.collection.mutable

class VertexMessageHandler(neighbours: mutable.Map[Int,ActorRef]) {

  val messageCount = new AtomicInteger(0)
   private val messageCache = mutable.Map[ActorRef, mutable.ArrayBuffer[VertexMessage]]()
  neighbours.foreach(neighbour => messageCache.put(neighbour._2, mutable.ArrayBuffer[VertexMessage]()))

  def sendMessage(message:VertexMessage):Unit = {
    messageCount.incrementAndGet()
    messageCache(getReaderJobWorker(message.vertexId))+=message
  }

  def flushMessages = {
    messageCache.foreach{
      case (neighbour,queue) =>
        neighbour ! VertexMessageBatch(queue.toArray)
    }
    neighbours.foreach(neighbour => messageCache.put(neighbour._2, mutable.ArrayBuffer[VertexMessage]()))
  }

  def getCountandReset():Int = messageCount.getAndSet(0)
  def getCount():Int = messageCount.get()

  def getReaderJobWorker(srcId: Long): ActorRef = {
    neighbours((srcId.abs % totalPartitions).toInt)
  }

}
object VertexMessageHandler {
  def apply(neighbours: mutable.Map[Int,ActorRef]) = {
    new VertexMessageHandler(neighbours)
  }
}
