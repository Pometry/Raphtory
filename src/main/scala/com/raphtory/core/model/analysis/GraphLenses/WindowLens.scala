package com.raphtory.core.model.analysis.GraphLenses

import akka.actor.ActorContext
import com.raphtory.api.ManagerCount
import com.raphtory.core.actors.PartitionManager.Workers.ViewJob
import com.raphtory.core.model.EntityStorage
import com.raphtory.core.model.analysis.entityVisitors.VertexVisitor
import com.raphtory.core.model.graphentities.Vertex
import kamon.Kamon

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ParIterable
import scala.collection.parallel.mutable.ParTrieMap

class WindowLens(
    viewJobOriginal: ViewJob,
    superstep: Int,
    workerID: Int,
    storage: EntityStorage,
    managerCount: ManagerCount
) extends GraphLens(viewJobOriginal, superstep, storage, managerCount) {
  var viewJobCurrent = viewJobOriginal
  val jobID = viewJobCurrent.jobID
  val timestamp = viewJobCurrent.timestamp
  val window = viewJobCurrent.window
  private var setWindow = window

  private val viewTimer = Kamon.gauge("Raphtory_View_Build_Time")
    .withTag("Partition",storage.managerID)
    .withTag("Worker",workerID)
    .withTag("JobID",jobID)
    .withTag("timestamp",timestamp)
  private val timetaken = System.currentTimeMillis()

  private var keySet: ParTrieMap[Long, Vertex] =
    storage.vertices.filter(v => v._2.aliveAtWithWindow(timestamp, setWindow)).map(v=> (v._1,v._2.viewAtWithWindow(timestamp,setWindow)))

  viewTimer.update(System.currentTimeMillis()-timetaken)

  private var TotalKeySize = 0
  private var firstCall    = true
  var timeTest             = ArrayBuffer[Long]()

  override def getVertices()(implicit context: ActorContext, managerCount: ManagerCount): ParIterable[VertexVisitor] = {
    if (firstCall) {
      TotalKeySize += keySet.size
      firstCall = false
    }
    keySet.map(v =>  new VertexVisitor(v._2, viewJobCurrent, superstep, this))
  }

  private var keySetMessages: ParIterable[VertexVisitor] = null
  private var messageFilter                            = false

  override def getMessagedVertices()(implicit context: ActorContext, managerCount: ManagerCount):ParIterable[VertexVisitor] = {
    if (!messageFilter) {
      keySetMessages = keySet.filter {
        case (id: Long, vertex: Vertex) => vertex.multiQueue.getMessageQueue(viewJobCurrent, superstep).nonEmpty
      }.map(v =>  new VertexVisitor(v._2, viewJobCurrent, superstep, this))
      TotalKeySize = keySetMessages.size + TotalKeySize
      messageFilter = true
    }
    keySetMessages
  }


  def shrinkWindow(newWindowSize: Long) = {
    setWindow = newWindowSize
    keySet = keySet.filter(v => (v._2).aliveAtWithWindow(timestamp, setWindow)).map(v=> (v._1,v._2.viewAtWithWindow(timestamp,setWindow)))
    messageFilter = false
    firstCall = true
    viewJobCurrent = ViewJob(viewJobCurrent.jobID,viewJobCurrent.timestamp,newWindowSize)
  }

  override def checkVotes(workerID: Int): Boolean =
    TotalKeySize == voteCount.get
}
