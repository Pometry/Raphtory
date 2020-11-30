package com.raphtory.core.analysis.API.GraphLenses

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorContext
import com.raphtory.core.analysis.API.ManagerCount
import com.raphtory.core.analysis.API.entityVisitors.VertexVisitor
import com.raphtory.core.components.PartitionManager.Workers.ViewJob
import com.raphtory.core.model.EntityStorage
import com.raphtory.core.model.graphentities.Vertex

import scala.collection.parallel.ParIterable

abstract class GraphLens(jobID: ViewJob, superstep: Int, storage: EntityStorage, managerCount: ManagerCount) {
  private val messages     = new AtomicInteger(0)
  protected var voteCount  = new AtomicInteger(0)
  def superStep() = superstep

  def getVertices()(implicit context: ActorContext, managerCount: ManagerCount): ParIterable[VertexVisitor] =
    storage.vertices.map(v =>  new VertexVisitor(v._2, jobID, superstep, this))

  def getMessagedVertices()(implicit context: ActorContext, managerCount: ManagerCount): ParIterable[VertexVisitor] =
    storage.vertices.filter {
      case (id: Long, vertex: Vertex) => vertex.multiQueue.getMessageQueue(jobID, superstep).nonEmpty
    }.map(v =>  new VertexVisitor(v._2, jobID, superstep, this))


  //TODO hide away
  def recordMessage() = messages.incrementAndGet()
  def getMessages() = messages.get
  def vertexVoted() = voteCount.incrementAndGet()
  def checkVotes(workerID: Int): Boolean = storage.vertices.size == voteCount.get
}
