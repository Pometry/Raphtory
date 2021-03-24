package com.raphtory.core.analysis.GraphLenses

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorContext
import com.raphtory.core.analysis.api.ManagerCount
import com.raphtory.core.actors.PartitionManager.Workers.ViewJob
import com.raphtory.core.model.EntityStorage
import com.raphtory.core.analysis.entity.Vertex
import com.raphtory.core.model.entities.RaphtoryVertex

import scala.collection.parallel.ParIterable

abstract class GraphLens(jobID: ViewJob, superstep: Int, storage: EntityStorage, managerCount: ManagerCount) {
  private val messages     = new AtomicInteger(0)
  protected var voteCount  = new AtomicInteger(0)
  def superStep() = superstep

  def getVertices()(implicit context: ActorContext, managerCount: ManagerCount): ParIterable[Vertex] =
    storage.vertices.map(v =>  new Vertex(v._2, jobID, superstep, this))

  def getMessagedVertices()(implicit context: ActorContext, managerCount: ManagerCount): ParIterable[Vertex] =
    storage.vertices.filter {
      case (id: Long, vertex: RaphtoryVertex) => vertex.multiQueue.getMessageQueue(jobID, superstep).nonEmpty
    }.map(v =>  new Vertex(v._2, jobID, superstep, this))


  //TODO hide away
  def recordMessage() = messages.incrementAndGet()
  def getMessages() = messages.get
  def vertexVoted() = voteCount.incrementAndGet()
  def checkVotes(workerID: Int): Boolean = storage.vertices.size == voteCount.get
}
