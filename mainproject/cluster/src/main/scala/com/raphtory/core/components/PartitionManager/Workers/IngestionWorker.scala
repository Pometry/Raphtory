package com.raphtory.core.components.PartitionManager.Workers

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.model.communication._
import com.raphtory.core.storage.EntityStorage

// TODO Re-add compression (removed during commit Log-Revamp)
class IngestionWorker(workerId: Int, storage: EntityStorage) extends Actor with ActorLogging {

  val mediator: ActorRef = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)

  override def preStart(): Unit =
    log.debug("IngestionWorker is being started.")

  override def receive: Receive = {
    case req: DstAddForOtherWorker       => processDstAddForOtherWorkerRequet(req)
    case req: DstResponseFromOtherWorker => processDstResponseFromOtherWorkerRequest(req)
    case req: DstWipeForOtherWorker      => processDstWipeForOtherWorkerRequest(req)
    case req: EdgeAdd                    => processEdgeAddRequest(req)
    case req: EdgeAddWithProperties      => processEdgeAddWithPropertiesRequest(req)
    case req: EdgeDelete                 => processEdgeDeleteRequest(req)
    case req: EdgeRemoveForOtherWorker   => processEdgeRemoveForOtherWorkerRequest(req)
    case req: RemoteEdgeAdd              => processRemoteEdgeAddRequest(req)
    case req: RemoteEdgeAddNew           => processRemoteEdgeAddNewRequest(req)
    case req: RemoteEdgeRemoval          => processRemoteEdgeRemovalRequest(req)
    case req: RemoteEdgeRemovalNew       => processRemoteEdgeRemovalNewRequest(req)
    case req: RemoteReturnDeaths         => processRemoteReturnDeathsRequest(req)
    case req: ReturnEdgeRemoval          => processReturnEdgeRemovalRequest(req)
    case req: VertexAdd                  => processVertexAddRequest(req)
    case req: VertexAddWithProperties    => processVertexAddWithPropertiesRequest(req)
    case req: VertexDelete               => processVertexDeleteRequest(req)
    case x =>
      log.warning(s"IngestionWorker [{}] received unknown [{}] message.", workerId, x)
  }

  def processVertexAddRequest(req: VertexAdd): Unit = {
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)

    storage.vertexAdd(req.msgTime, req.srcID, vertexType = req.vType)
    vHandle(req.srcID, req.msgTime)
  }

  def processVertexDeleteRequest(req: VertexDelete): Unit = {
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)

    storage.vertexRemoval(req.msgTime, req.srcID)
    vHandle(req.srcID, req.msgTime)
  }

  def processVertexAddWithPropertiesRequest(req: VertexAddWithProperties): Unit = {
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)

    storage.vertexAdd(req.msgTime, req.srcID, req.properties, req.vType)
    vHandle(req.srcID, req.msgTime)
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

  def processEdgeAddRequest(req: EdgeAdd): Unit = {
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)

    storage.edgeAdd(req.msgTime, req.srcID, req.dstID, edgeType = req.eType)
    eHandle(req.srcID, req.dstID, req.msgTime)
  }

  def processEdgeAddWithPropertiesRequest(req: EdgeAddWithProperties): Unit = {
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)

    storage.edgeAdd(req.msgTime, req.srcID, req.dstID, req.properties, req.eType)
    eHandle(req.srcID, req.dstID, req.msgTime)
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

  def processEdgeDeleteRequest(req: EdgeDelete): Unit = {
    log.debug("IngestionWorker [{}] received [{}] request.", workerId, req)

    storage.edgeRemoval(req.msgTime, req.srcID, req.dstID)
    eHandle(req.srcID, req.dstID, req.msgTime)
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

  private def vHandle(srcID: Long, msgTime: Long): Unit = {
    storage.timings(msgTime)
    storage.messageCount(workerId) += 1
  }

  private def eHandle(srcID: Long, dstID: Long, msgTime: Long): Unit = {
    storage.timings(msgTime)
    storage.messageCount(workerId) += 1
  }

  private def eHandleSecondary(srcID: Long, dstID: Long, msgTime: Long): Unit = {
    storage.timings(msgTime)
    storage.secondaryMessageCount(workerId) += 1
  }

  private def wHandle(): Unit =
    storage.workerMessageCount(workerId) += 1
}
