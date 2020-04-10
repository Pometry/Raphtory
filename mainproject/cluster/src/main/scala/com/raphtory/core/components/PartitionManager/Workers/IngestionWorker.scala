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
    case req: DstAddForOtherWorker       => processDstAddForOtherWorkerRequet(req)
    case req: DstResponseFromOtherWorker => processDstResponseFromOtherWorkerRequest(req)
    case req: DstWipeForOtherWorker      => processDstWipeForOtherWorkerRequest(req)
    case req: TrackedEdgeAdd                    => processEdgeAddRequest(req)
    case req: TrackedEdgeAddWithProperties      => processEdgeAddWithPropertiesRequest(req)
    case req: TrackedEdgeDelete                 => processEdgeDeleteRequest(req)
    case req: EdgeRemoveForOtherWorker   => processEdgeRemoveForOtherWorkerRequest(req)
    case req: RemoteEdgeAdd              => processRemoteEdgeAddRequest(req)
    case req: RemoteEdgeAddNew           => processRemoteEdgeAddNewRequest(req)
    case req: RemoteEdgeRemoval          => processRemoteEdgeRemovalRequest(req)
    case req: RemoteEdgeRemovalNew       => processRemoteEdgeRemovalNewRequest(req)
    case req: RemoteReturnDeaths         => processRemoteReturnDeathsRequest(req)
    case req: ReturnEdgeRemoval          => processReturnEdgeRemovalRequest(req)
    case req: TrackedVertexAdd                  => processVertexAddRequest(req)
    case req: TrackedVertexAddWithProperties    => processVertexAddWithPropertiesRequest(req)
    case req: TrackedVertexDelete               => processVertexDeleteRequest(req)
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

  def processVertexDeleteRequest(req: TrackedVertexDelete): Unit = {
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)
    val update = req.update
    storage.vertexRemoval(update.msgTime, update.srcID)
    vertexDeleteTrack(update.srcID, update.msgTime,req.routerID,req.messageID)
  }

  def processVertexAddWithPropertiesRequest(req: TrackedVertexAddWithProperties): Unit = {
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)
    val update = req.update
    storage.vertexAdd(update.msgTime, update.srcID, update.properties, update.vType)
    vertexAddTrack(update.srcID, update.msgTime,req.routerID,req.messageID)
  }

  def processEdgeAddRequest(req: TrackedEdgeAdd): Unit = {
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)
    val update = req.update
    storage.edgeAdd(update.msgTime, update.srcID, update.dstID, edgeType = update.eType)
    eHandle(update.srcID, update.dstID, update.msgTime)
  }

  def processEdgeAddWithPropertiesRequest(req: TrackedEdgeAddWithProperties): Unit = {
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)
    val update = req.update
    storage.edgeAdd(update.msgTime, update.srcID, update.dstID, update.properties, update.eType)
    eHandle(update.srcID, update.dstID, update.msgTime)
  }

  def processEdgeDeleteRequest(req: TrackedEdgeDelete): Unit = {
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)
    val update = req.update
    storage.edgeRemoval(update.msgTime, update.srcID, update.dstID)
    eHandle(update.srcID, update.dstID, update.msgTime)
  }



  def processDstAddForOtherWorkerRequet(req: DstAddForOtherWorker): Unit = {
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)

    storage.vertexWorkerRequest(req.msgTime, req.dstID, req.srcForEdge, req.edge, req.present)
    wHandle()
  }

  def processDstWipeForOtherWorkerRequest(req: DstWipeForOtherWorker): Unit = {
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)

    storage.vertexWipeWorkerRequest(req.msgTime, req.dstID, req.srcForEdge, req.edge, req.present)
    wHandle()
  }

  def processDstResponseFromOtherWorkerRequest(req: DstResponseFromOtherWorker): Unit = {
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)

    storage.vertexWorkerRequestEdgeHandler(req.msgTime, req.srcForEdge, req.dstID, req.removeList)
    wHandle()
  }

  def processEdgeRemoveForOtherWorkerRequest(req: EdgeRemoveForOtherWorker): Unit = {
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)

    storage.edgeRemovalFromOtherWorker(req.msgTime, req.srcID, req.dstID)
    wHandle()
  }



  def processRemoteEdgeAddRequest(req: RemoteEdgeAdd): Unit = {
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)

    storage.remoteEdgeAdd(req.msgTime, req.srcID, req.dstID, req.properties, req.eType)

    eHandleSecondary(req.srcID, req.dstID, req.msgTime)
  }

  def processRemoteEdgeAddNewRequest(req: RemoteEdgeAddNew): Unit = {
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)

    storage.remoteEdgeAddNew(req.msgTime, req.srcID, req.dstID, req.properties, req.kills, req.vType)

    eHandleSecondary(req.srcID, req.dstID, req.msgTime)
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

  def processRemoteReturnDeathsRequest(req: RemoteReturnDeaths): Unit = {
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)

    storage.remoteReturnDeaths(req.msgTime, req.srcID, req.dstID, req.kills)
    eHandleSecondary(req.srcID, req.dstID, req.msgTime)
  }

  private def vertexAddTrack(srcID: Long, msgTime: Long,routerID:String,routerTime:Int): Unit = {
    storage.timings(msgTime)
    queuedMessageMap.get(routerID) match {
      case Some(queue) => queue += queueItem(routerTime,msgTime)
      case None =>
        val queue = new mutable.PriorityQueue[queueItem]()
        queue += queueItem(routerTime,msgTime)
        queuedMessageMap put(routerID,queue)
    }
  }

  private def vertexDeleteTrack(srcID: Long, msgTime: Long,routerID:String,routerTime:Int): Unit = {
    storage.timings(msgTime)
  }

  private def eHandle(srcID: Long, dstID: Long, msgTime: Long): Unit = {
    storage.timings(msgTime)
  }

  private def eHandleSecondary(srcID: Long, dstID: Long, msgTime: Long): Unit = {
    storage.timings(msgTime)
  }

  private def wHandle(): Unit = {}


  private def processWatermarkRequest() ={
    queuedMessageMap.foreach(queue => {
      //println(s"${workerId} ${queue._1} ${queue._2.dequeueAll}")
    })
  }


  private def scheduleTasks(): Unit = {
    log.debug("Preparing to schedule tasks in Spout.")

    val watermarkCancellable =
      SchedulerUtil.scheduleTask(initialDelay = 7 seconds, interval = 1 second, receiver = self, message = "watermark")
    scheduledTaskMap.put("watermark", watermarkCancellable)

  }

  }

