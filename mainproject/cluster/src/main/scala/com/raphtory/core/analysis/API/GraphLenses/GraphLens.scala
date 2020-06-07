package com.raphtory.core.analysis.API.GraphLenses

import akka.actor.ActorContext
import com.raphtory.core.analysis.API.ManagerCount
import com.raphtory.core.analysis.API.entityVisitors.VertexVisitor
import com.raphtory.core.model.graphentities.Vertex
import com.raphtory.core.storage.EntityStorage
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.parallel.mutable.ParTrieMap

abstract class GraphLens(
    jobID: String,
    superstep: Int,
    timestamp: Long,
    windowsize: Long,
    workerID: Int,
    storage: EntityStorage,
    managerCount: ManagerCount
) {
  private val messages = new AtomicInteger(0)

  protected var voteCount                             = new AtomicInteger(0)
  def job()                                   = jobID
  def superStep()                                = superstep
  def getVerticesSet(): ParTrieMap[Long, Vertex] = storage.vertices

  def getVerticesWithMessages(): ParTrieMap[Long, Vertex] = storage.vertices.filter {
    case (id: Long, vertex: Vertex) => vertex.multiQueue.getMessageQueue(job(), superstep).nonEmpty
  }

  def recordMessage(sourceID: Long, vertexID: Long, data: Any) =
    messages.incrementAndGet()

  def getMessages() = messages.get

  def getVertex(v: Vertex)(implicit context: ActorContext, managerCount: ManagerCount): VertexVisitor =
    new VertexVisitor(v, jobID, superstep, this, timestamp, windowsize)

  def getTotalVerticesSet() =
    storage.vertices.keySet

  def latestTime: Long =
    storage.newestTime

  def vertexVoted() =
    voteCount.incrementAndGet()

  def checkVotes(workerID: Int): Boolean =
    storage.vertices.size == voteCount.get
}
