package com.raphtory.core.analysis.API.GraphLenses

import akka.actor.ActorContext
import com.raphtory.core.analysis.API.ManagerCount
import com.raphtory.core.analysis.API.entityVisitors.VertexVisitor
import com.raphtory.core.model.graphentities.Vertex
import com.raphtory.core.storage.EntityStorage
import monix.execution.atomic.AtomicInt

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
  private var messages = AtomicInt(0)
  //val messageQueues:ParTrieMap[String,ConcurrentHashMap.KeySetView[(Long,Long,Any),java.lang.Boolean]] = ParTrieMap[String,ConcurrentHashMap.KeySetView[(Long,Long,Any),java.lang.Boolean]]()
  //Utils.getAllReaders(managerCount.count).foreach(reader => messageQueues put (reader,java.util.concurrent.ConcurrentHashMap.newKeySet[(Long,Long,Any)]()))

  protected var voteCount                        = AtomicInt(0)
  def job()                                      = jobID
  def superStep()                                = superstep
  def getVerticesSet(): ParTrieMap[Long, Vertex] = storage.vertices

  def getVerticesWithMessages(): ParTrieMap[Long, Vertex] = storage.vertices.filter {
    case (id: Long, vertex: Vertex) => vertex.multiQueue.getMessageQueue(job(), superstep).nonEmpty
  }

  def recordMessage(sourceID: Long, vertexID: Long, data: Any) =
    messages.increment()

  def getMessages() = messages.get

  def getVertex(v: Vertex)(implicit context: ActorContext, managerCount: ManagerCount): VertexVisitor =
    new VertexVisitor(v, jobID, superstep, this, timestamp, windowsize)

  def getTotalVerticesSet() =
    storage.vertices.keySet

  def latestTime: Long = storage.newestTime

  def vertexVoted() = voteCount.increment()

  def checkVotes(workerID: Int): Boolean =
    //
    storage.vertices.size == voteCount.get
}
