package com.raphtory.core.analysis

import java.util.concurrent.atomic.AtomicInteger

import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.actors.PartitionManager.Workers.AnalysisSubtaskWorker
import com.raphtory.core.analysis.entity.Vertex
import com.raphtory.core.model.EntityStorage
import com.raphtory.core.model.communication.{VertexMessage, VertexMessageHandler}
import kamon.Kamon

import scala.collection.parallel.ParIterable
import scala.collection.parallel.mutable.ParTrieMap

final case class GraphLens(
    jobId: String,
    timestamp: Long,
    window: Option[Long],
    var superStep: Int,
    private val workerId: Int,
    private val storage: EntityStorage,
    private val messageHandler:VertexMessageHandler
) {
  private var voteCount            = new AtomicInteger(0)
  private var vertexCount          = new AtomicInteger(0)

  private val viewTimer = Kamon
    .gauge("Raphtory_View_Build_Time")
    .withTag("Partition", storage.managerID)
    .withTag("Worker", workerId)
    .withTag("JobID", jobId)
    .withTag("timestamp", timestamp)

  private lazy val vertexMap: ParTrieMap[Long, Vertex] = {
    val startTime = System.currentTimeMillis()
    val result = window match {
      case None =>
        storage.vertices.collect {
          case (k, v) if v.aliveAt(timestamp) =>
            k -> v.viewAt(timestamp,this)
        }
      case Some(w) => {
        storage.vertices.collect {
          case (k, v) if v.aliveAtWithWindow(timestamp, w) =>
            k -> v.viewAtWithWindow(timestamp, w,this)
        }
    }
    }
    viewTimer.update(System.currentTimeMillis() - startTime)
    result
  }
  def getVertices(): ParIterable[Vertex] = {
    vertexCount.set(vertexMap.size)
    vertexMap.map(x=>x._2)
  }
  def getMessagedVertices(): ParIterable[Vertex] = {
    val startTime = System.currentTimeMillis()
    val result    = vertexMap.collect {
      case (k, vertex) if vertex.hasMessage() => vertex
    }
    viewTimer.update(System.currentTimeMillis() - startTime)
    vertexCount.set(result.size)
    result
  }

  def checkVotes(): Boolean = vertexCount.get() == voteCount.get()

  //TODO hide away
  def sendMessage(msg: VertexMessage): Unit = messageHandler.sendMessage(msg)


  def vertexVoted(): Unit = voteCount.incrementAndGet()
  def nextStep(): Unit    = {
    voteCount.set(0)
    vertexCount.set(0)
    superStep += 1
  }
  def receiveMessage(msg: VertexMessage): Unit = {
      vertexMap(msg.vertexId).receiveMessage(msg)
  }
}
