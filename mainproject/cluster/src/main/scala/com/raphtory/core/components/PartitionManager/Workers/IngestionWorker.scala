package com.raphtory.core.components.PartitionManager.Workers

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.model.communication._
import com.raphtory.core.storage.EntityStorage
import com.raphtory.core.utils.SchedulerUtil

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap

case class queueItem(routerEpoch:Int,timestamp:Long)extends Ordered[queueItem] {
  def compare(that: queueItem): Int = that.routerEpoch-this.routerEpoch
}

// TODO Re-add compression (removed during commit Log-Revamp)
class IngestionWorker(workerId: Int, storage: EntityStorage) extends Actor with ActorLogging {

  val mediator: ActorRef = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)

  private val queuedMessageMap = ParTrieMap[String, mutable.PriorityQueue[queueItem]]()
  private val safeMessageMap = ParTrieMap[String, queueItem]()
  private val scheduledTaskMap: mutable.HashMap[String, Cancellable] = mutable.HashMap[String, Cancellable]()

  override def preStart() {
    log.debug("IngestionWorker is being started.")
    scheduleTasks()
  }
  override def postStop() {
    val allTasksCancelled = scheduledTaskMap.forall {
      case (key, task) =>
        SchedulerUtil.cancelTask(key, task)
    }
    if (!allTasksCancelled) log.warning("Failed to cancel all scheduled tasks post stop.")
  }

  override def receive: Receive = {
    case req: TrackedVertexAdd                  => processVertexAddRequest(req)//Add a new vertex
    case req: TrackedVertexAddWithProperties    => processVertexAddWithPropertiesRequest(req)//Add a new vertex with properties
    case req: TrackedEdgeAdd                    => processEdgeAddRequest(req)//Add an edge
    case req: TrackedEdgeAddWithProperties      => processEdgeAddWithPropertiesRequest(req)// Add an edge with properties

    case req: RemoteEdgeAddNew           => processRemoteEdgeAddNewRequest(req)//A writer has requested a new edge sync for a destination node in this worker
    case req: RemoteEdgeAdd              => processRemoteEdgeAddRequest(req)// A writer has requested an existing edge sync for a destination node on in this worker
    case req: RemoteReturnDeaths         => processRemoteReturnDeathsRequest(req)//The remote worker has returned all removals in the destination node -- for new edges
    case req: EdgeSyncAck                => processEdgeSyncAck(req) //The remote worker acknowledges the completion of an edge sync

    case req: DstAddForOtherWorker       => processDstAddForOtherWorkerRequet(req) //A local writer has requested a new edge sync for a destination node in this worker
    case req: DstResponseFromOtherWorker => processDstResponseFromOtherWorkerRequest(req)//The local writer has responded with the deletions for local split edge to allow the main writer to insert them

    case req: TrackedEdgeDelete          => processEdgeDeleteRequest(req)
    case req: RemoteEdgeRemovalNew       => processRemoteEdgeRemovalNewRequest(req)
    case req: RemoteEdgeRemoval          => processRemoteEdgeRemovalRequest(req)

    case req: EdgeRemoveForOtherWorker   => processEdgeRemoveForOtherWorkerRequest(req)
    case req: DstWipeForOtherWorker      => processDstWipeForOtherWorkerRequest(req)

    case req: TrackedVertexDelete        => processVertexDeleteRequest(req)
    case req: ReturnEdgeRemoval          => processReturnEdgeRemovalRequest(req)

    case "watermark"                     => processWatermarkRequest()
    case x =>
      log.warning(s"IngestionWorker [{}] received unknown [{}] message.", workerId, x)
  }


  def processVertexAddRequest(req: TrackedVertexAdd): Unit = {
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)
    val update = req.update
    storage.vertexAdd(update.msgTime, update.srcID, vertexType = update.vType)
    vertexAddTrack(update.srcID, update.msgTime,req.routerID,req.messageID)
  }

  def processVertexAddWithPropertiesRequest(req: TrackedVertexAddWithProperties): Unit = {
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)
    val update = req.update
    storage.vertexAdd(update.msgTime, update.srcID, update.properties, update.vType)
    vertexAddTrack(update.srcID, update.msgTime,req.routerID,req.messageID)
  }

  private def vertexAddTrack(srcID: Long, msgTime: Long,routerID:String,routerTime:Int): Unit = {
      //Vertex Adds the message time straight into queue as no sync
    storage.timings(msgTime)
    addToWatermarkQueue(routerID,routerTime,msgTime)
  }




  def processEdgeAddRequest(req: TrackedEdgeAdd): Unit = {
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)
    val update = req.update
    val local = storage.edgeAdd(update.msgTime, update.srcID, update.dstID,req.routerID,req.messageID, edgeType = update.eType)
    edgeAddTrack(update.srcID, update.dstID, update.msgTime,local,req.routerID,req.messageID)
  }

  def processEdgeAddWithPropertiesRequest(req: TrackedEdgeAddWithProperties): Unit = {
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)
    val update = req.update
    val local = storage.edgeAdd(update.msgTime, update.srcID, update.dstID,req.routerID,req.messageID, update.properties, update.eType)
    edgeAddTrack(update.srcID, update.dstID, update.msgTime,local,req.routerID,req.messageID)
  }

  private def edgeAddTrack(srcID: Long, dstID: Long, msgTime: Long,local:Boolean,routerID:String,routerTime:Int): Unit = {
    storage.timings(msgTime)
    if(local) //if the edge is totally handled by this worker then we are safe to add to watermark queue
      addToWatermarkQueue(routerID,routerTime,msgTime)
  }

  def processRemoteEdgeAddRequest(req: RemoteEdgeAdd): Unit = {
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)
    storage.remoteEdgeAdd(req.msgTime, req.srcID, req.dstID, req.properties, req.eType,req.routerID,req.routerTime)
    remoteEdgeAddTrack(req.msgTime)
  }

  def processRemoteEdgeAddNewRequest(req: RemoteEdgeAddNew): Unit = {
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)
    storage.remoteEdgeAddNew(req.msgTime, req.srcID, req.dstID, req.properties, req.kills, req.vType,req.routerID,req.routerTime)
    remoteEdgeAddTrack(req.msgTime)
  }

  private def remoteEdgeAddTrack(msgTime: Long): Unit = {
    storage.timings(msgTime)
  }
//
  def processRemoteReturnDeathsRequest(req: RemoteReturnDeaths): Unit = { //when the new edge add is responded to we can say it is synced
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)
    storage.remoteReturnDeaths(req.msgTime, req.srcID, req.dstID, req.kills)
    addToWatermarkQueue(req.routerID,req.routerTime,req.msgTime)
  }

  def processEdgeSyncAck(req: EdgeSyncAck) = { //when the edge isn't new we will get this response instead
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)
    addToWatermarkQueue(req.routerID,req.routerTime,req.msgTime)
  }

  def processDstAddForOtherWorkerRequet(req: DstAddForOtherWorker): Unit = { //local worker asking this one to deal with an incoming edge
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)
    storage.vertexWorkerRequest(req.msgTime, req.dstID, req.srcForEdge, req.edge, req.present,req.routerID,req.routerTime)
  }

  def processDstResponseFromOtherWorkerRequest(req: DstResponseFromOtherWorker): Unit = { //local worker responded for a new edge so can watermark, if existing edge will just be an ack
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)
    storage.vertexWorkerRequestEdgeHandler(req.msgTime, req.srcForEdge, req.dstID, req.removeList)
    addToWatermarkQueue(req.routerID,req.routerTime,req.msgTime)
  }


  def processEdgeDeleteRequest(req: TrackedEdgeDelete): Unit = {
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)
    val update = req.update
    storage.edgeRemoval(update.msgTime, update.srcID, update.dstID)
    eHandle(update.srcID, update.dstID, update.msgTime)
  }



  def processVertexDeleteRequest(req: TrackedVertexDelete): Unit = {
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)
    val update = req.update
    storage.vertexRemoval(update.msgTime, update.srcID)
    vertexDeleteTrack(update.srcID, update.msgTime,req.routerID,req.messageID)
  }
  private def vertexDeleteTrack(srcID: Long, msgTime: Long,routerID:String,routerTime:Int): Unit = {
    storage.timings(msgTime)
  }








  def processDstWipeForOtherWorkerRequest(req: DstWipeForOtherWorker): Unit = {
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)

    storage.vertexWipeWorkerRequest(req.msgTime, req.dstID, req.srcForEdge, req.edge, req.present)
    wHandle()
  }



  def processEdgeRemoveForOtherWorkerRequest(req: EdgeRemoveForOtherWorker): Unit = {
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)

    storage.edgeRemovalFromOtherWorker(req.msgTime, req.srcID, req.dstID)
    wHandle()
  }






  def processRemoteEdgeRemovalRequest(req: RemoteEdgeRemoval): Unit = {
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)

    storage.remoteEdgeRemoval(req.msgTime, req.srcID, req.dstID)
    eHandleSecondary(req.srcID, req.dstID, req.msgTime)
  }

  def processRemoteEdgeRemovalNewRequest(req: RemoteEdgeRemovalNew): Unit = {
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)

    storage.remoteEdgeRemovalNew(req.msgTime, req.srcID, req.dstID, req.kills)
    eHandleSecondary(req.srcID, req.dstID, req.msgTime)
  }

  def processReturnEdgeRemovalRequest(req: ReturnEdgeRemoval): Unit = {
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)

    storage.returnEdgeRemoval(req.msgTime, req.srcID, req.dstID)
    eHandleSecondary(req.srcID, req.dstID, req.msgTime)
  }








  private def addToWatermarkQueue(routerID:String,routerTime:Int,msgTime:Long) = {
    queuedMessageMap.get(routerID) match {
      case Some(queue) => queue += queueItem(routerTime,msgTime)
      case None =>
        val queue = new mutable.PriorityQueue[queueItem]()
        queue += queueItem(routerTime,msgTime)
        queuedMessageMap put(routerID,queue)
    }
  }

  private def processWatermarkRequest() ={
    if(queuedMessageMap nonEmpty)
      queuedMessageMap.map(queue => {
        setSafePoint(queue._1,queue._2).timestamp
      }).min
    else 0L
  }

  private def setSafePoint(routerName:String,messageQueue:mutable.PriorityQueue[queueItem]) = {
    val default = queueItem(-1,0)
    var currentSafePoint = safeMessageMap.get(routerName) match {
      case Some(value) => value
      case None => default
    }
    if(messageQueue nonEmpty)
      while(messageQueue.headOption.getOrElse(default).routerEpoch==currentSafePoint.routerEpoch+1)
         currentSafePoint = messageQueue.dequeue()
    safeMessageMap put (routerName,currentSafePoint)
    currentSafePoint
  }


  private def scheduleTasks(): Unit = {
    log.debug("Preparing to schedule tasks in Spout.")

    val watermarkCancellable =
      SchedulerUtil.scheduleTask(initialDelay = 7 seconds, interval = 1 second, receiver = self, message = "watermark")
    scheduledTaskMap.put("watermark", watermarkCancellable)

  }

  }

