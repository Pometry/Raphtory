package com.raphtory.core.analysis.API.GraphLenses

import akka.actor.ActorContext
import com.raphtory.core.analysis.API.ManagerCount
import com.raphtory.core.analysis.API.entityVisitors.VertexVisitor
import com.raphtory.core.components.PartitionManager.Workers.ViewJob
import com.raphtory.core.model.graphentities.Vertex
import com.raphtory.core.storage.EntityStorage
import kamon.Kamon

import scala.collection.parallel.ParIterable

class ViewLens(
                jobID: ViewJob,
                superstep: Int,
                workerID: Int,
                storage: EntityStorage,
                managerCount: ManagerCount
) extends GraphLens(jobID, superstep, storage, managerCount) {

  private var keySet: ParIterable[VertexVisitor]          = ParIterable[VertexVisitor]()
  private var keySetMessages: ParIterable[VertexVisitor]  = ParIterable[VertexVisitor]()
  private var messageFilter                               = false
  private var firstRun                                    = true

  private val viewTimer = Kamon.gauge("Raphtory_View_Build_Time")
    .withTag("Partition",storage.managerID)
    .withTag("Worker",workerID)
    .withTag("JobID",jobID.jobID)
    .withTag("timestamp",jobID.timestamp)

  override def getMessagedVertices()(implicit context: ActorContext, managerCount: ManagerCount):  ParIterable[VertexVisitor] = {
    val timetaken = System.currentTimeMillis()
    if (!messageFilter) {
      keySetMessages = storage.vertices.filter {
        case (id: Long, vertex: Vertex) =>
          vertex.aliveAt(jobID.timestamp) && vertex.multiQueue.getMessageQueue(jobID, superstep).nonEmpty
      }.map(v =>  new VertexVisitor(v._2.viewAt(jobID.timestamp), jobID, superstep, this))
      messageFilter = true
    }
    viewTimer.update(System.currentTimeMillis()-timetaken)
    keySetMessages
  }

  override def getVertices()(implicit context: ActorContext, managerCount: ManagerCount): ParIterable[VertexVisitor] = {
    if (firstRun) {
      val timetaken = System.currentTimeMillis()
      keySet = storage.vertices.filter(v => v._2.aliveAt(jobID.timestamp)).map(v =>  new VertexVisitor(v._2.viewAt(jobID.timestamp), jobID, superstep, this))
      firstRun = false
      viewTimer.update(System.currentTimeMillis()-timetaken)
    }
    keySet
  }
  //override def getVertex(id : Long)(implicit context : ActorContext, managerCount : ManagerCount) : VertexVisitor = new VertexVisitor(keySet(id.toInt).viewAt(timestamp),job(),superstep,this,timestamp,-1)


  override def checkVotes(workerID: Int): Boolean =
//    println(workerID +" "+ messageFilter)
    if (messageFilter)
      keySetMessages.size == voteCount.get
    else
      keySet.size == voteCount.get
}
