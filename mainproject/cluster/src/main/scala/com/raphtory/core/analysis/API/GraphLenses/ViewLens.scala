package com.raphtory.core.analysis.API.GraphLenses

import akka.actor.ActorContext
import com.raphtory.core.analysis.API.ManagerCount
import com.raphtory.core.analysis.API.entityVisitors.VertexVisitor
import com.raphtory.core.model.graphentities.Vertex
import com.raphtory.core.storage.EntityStorage
import kamon.Kamon

import scala.collection.parallel.mutable.ParTrieMap

class ViewLens(
    jobID: String,
    superstep: Int,
    timestamp: Long,
    workerID: Int,
    storage: EntityStorage,
    managerCount: ManagerCount
) extends GraphLens(jobID, superstep, timestamp, -1, workerID, storage, managerCount) {

  override def job()                                   = jobID + timestamp
  private var keySet: ParTrieMap[Long, Vertex]         = ParTrieMap[Long, Vertex]()
  private var keySetMessages: ParTrieMap[Long, Vertex] = ParTrieMap[Long, Vertex]()
  private var messageFilter                            = false
  private var firstRun                                 = true

  override def getVerticesWithMessages(): ParTrieMap[Long, Vertex] = {
    val viewTimer = Kamon.timer("Raphtory_View_Build_Time")
      .withTag("Partition",storage.managerID)
      .withTag("Worker",workerID)
      .withTag("JobID",jobID)
      .withTag("SuperStep",superstep)
      .withTag("timestamp",timestamp)
      .start()
    if (!messageFilter) {
      keySetMessages = storage.vertices.filter {
        case (id: Long, vertex: Vertex) =>
          vertex.aliveAt(timestamp) && vertex.multiQueue.getMessageQueue(job(), superstep).nonEmpty
      }
      messageFilter = true
    }
    viewTimer.stop()
    keySetMessages
  }

  override def getVerticesSet(): ParTrieMap[Long, Vertex] = {
    if (firstRun) {
      val viewTimer = Kamon.timer("Raphtory_View_Build_Time")
        .withTag("Partition",storage.managerID)
        .withTag("Worker",workerID)
        .withTag("JobID",jobID)
        .withTag("SuperStep",superstep)
        .withTag("timestamp",timestamp)
        .start()
      keySet = storage.vertices.filter(v => v._2.aliveAt(timestamp))
      firstRun = false
      viewTimer.stop()
    }
    keySet
  }
  //override def getVertex(id : Long)(implicit context : ActorContext, managerCount : ManagerCount) : VertexVisitor = new VertexVisitor(keySet(id.toInt).viewAt(timestamp),job(),superstep,this,timestamp,-1)
  override def getVertex(v: Vertex)(implicit context: ActorContext, managerCount: ManagerCount): VertexVisitor =
    new VertexVisitor(v.viewAt(timestamp), job(), superstep, this, timestamp, -1)
  override def latestTime: Long = timestamp

  override def checkVotes(workerID: Int): Boolean =
//    println(workerID +" "+ messageFilter)
    if (messageFilter)
      keySetMessages.size == voteCount.get
    else
      keySet.size == voteCount.get
}
