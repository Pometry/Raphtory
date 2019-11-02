package com.raphtory.core.components.PartitionManager.Workers

import akka.actor.{Actor, ActorRef}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.datastax.driver.core.exceptions.WriteTimeoutException
import com.raphtory.core.model.communication._
import com.raphtory.core.model.graphentities.{Edge, RemoteEdge, Vertex}
import com.raphtory.core.storage.{EntityStorage, RaphtoryDBWrite}
import com.raphtory.core.utils.Utils

class IngestionWorker(workerID:Int,storage:EntityStorage) extends Actor {
  val mediator: ActorRef = DistributedPubSub(context.system).mediator // get the mediator for sending cluster messages
  mediator ! DistributedPubSubMediator.Put(self)
  val compressing: Boolean = Utils.compressing
  val saving: Boolean = Utils.saving

  override def receive: Receive = {
    case VertexAdd(routerID, msgTime, srcId) => storage.vertexAdd(routerID, workerID, msgTime, srcId)
      vHandle(srcId, msgTime)
    case VertexRemoval(routerID, msgTime, srcId) => storage.vertexRemoval(routerID, workerID, msgTime, srcId)
      vHandle(srcId, msgTime)
    case VertexAddWithProperties(routerID, msgTime, srcId, properties) => storage.vertexAdd(routerID, workerID, msgTime, srcId, properties)
      vHandle(srcId, msgTime)

    case DstAddForOtherWorker(routerID, msgTime, dstID, srcForEdge, edge, present) => storage.vertexWorkerRequest(routerID, workerID, msgTime, dstID, srcForEdge, edge, present)
      wHandle()
    case DstWipeForOtherWorker(routerID, msgTime, dstID, srcForEdge, edge, present) => storage.vertexWipeWorkerRequest(routerID, workerID, msgTime, dstID, srcForEdge, edge, present)
      wHandle()
    case DstResponseFromOtherWorker(routerID, msgTime, srcForEdge, dstID, removeList) => storage.vertexWorkerRequestEdgeHandler(routerID, workerID, msgTime, srcForEdge, dstID, removeList)
      wHandle()
    case EdgeRemoveForOtherWorker(routerID, msgTime, srcID, dstID) => storage.edgeRemovalFromOtherWorker(routerID, workerID, msgTime, srcID, dstID)
      wHandle()
    //case EdgeRemovalAfterArchiving(routerID,msgTime,srcID,dstID)           => EntityStorage.edgeRemovalAfterArchiving(routerID,workerID,msgTime,srcID,dstID) //disabled at the moment

    case EdgeAdd(routerID, msgTime, srcId, dstId) => storage.edgeAdd(routerID, workerID, msgTime, srcId, dstId)
      eHandle(srcId, dstId, msgTime)
    case EdgeAddWithProperties(routerID, msgTime, srcId, dstId, properties) => storage.edgeAdd(routerID, workerID, msgTime, srcId, dstId, properties)
      eHandle(srcId, dstId, msgTime)

    case RemoteEdgeAdd(routerID, msgTime, srcId, dstId, properties) => storage.remoteEdgeAdd(routerID, workerID, msgTime, srcId, dstId, properties)
      eHandleSecondary(srcId, dstId, msgTime)
    case RemoteEdgeAddNew(routerID, msgTime, srcId, dstId, properties, deaths) => storage.remoteEdgeAddNew(routerID, workerID, msgTime, srcId, dstId, properties, deaths)
      eHandleSecondary(srcId, dstId, msgTime)

    case EdgeRemoval(routerID, msgTime, srcId, dstId) => storage.edgeRemoval(routerID, workerID, msgTime, srcId, dstId)
      eHandle(srcId, dstId, msgTime)
    case RemoteEdgeRemoval(routerID, msgTime, srcId, dstId) => storage.remoteEdgeRemoval(routerID, workerID, msgTime, srcId, dstId)
      eHandleSecondary(srcId, dstId, msgTime)
    case RemoteEdgeRemovalNew(routerID, msgTime, srcId, dstId, deaths) => storage.remoteEdgeRemovalNew(routerID, workerID, msgTime, srcId, dstId, deaths)
      eHandleSecondary(srcId, dstId, msgTime)

    case ReturnEdgeRemoval(routerID, msgTime, srcId, dstId) => storage.returnEdgeRemoval(routerID, workerID, msgTime, srcId, dstId)
      eHandleSecondary(srcId, dstId, msgTime)
    case RemoteReturnDeaths(routerID, msgTime, srcId, dstId, deaths) => storage.remoteReturnDeaths(routerID, workerID, msgTime, srcId, dstId, deaths)
      eHandleSecondary(srcId, dstId, msgTime)

    case CompressVertex(id, time) => compressVertex(id, time)
    case ArchiveVertex(id, compressTime, archiveTime) => archiveVertex(id, compressTime, archiveTime)
    case ArchiveOnlyVertex(id, archiveTime) => archiveOnlyVertex(id, archiveTime)
  }

  /** ***********************************************************
    * LOG HANDLING SECTION                     *
    * ***********************************************************/

  def vHandle(srcID: Int, msgTime: Long): Unit = {
    storage.timings(msgTime)
    storage.messageCount(workerID) += 1
  }

  def eHandle(srcID: Int, dstID: Int, msgTime: Long): Unit = {
    storage.timings(msgTime)
    storage.messageCount(workerID) += 1
  }

  def eHandleSecondary(srcID: Int, dstID: Int, msgTime: Long): Unit = {
    storage.timings(msgTime)
    storage.secondaryMessageCount(workerID) += 1
  }

  def wHandle(): Unit = {
    storage.workerMessageCount(workerID) += 1
  }

  /** ***********************************************************
    * COMPRESSION SECTION                    *
    * ***********************************************************/
//TODO compress into one function
  def compressVertex(key: Int, now: Long) = {
    storage.vertices.get(key) match {
      case Some(vertex) => {
        saveVertex(vertex, now)
        saveEdges(vertex, now, -1, true, false)
      }
      case None =>
    }
    sender() ! FinishedVertexCompression(key)
  }

  def archiveVertex(key: Int, compressTime: Long, archiveTime: Long) = {
    storage.vertices.get(key) match {
      case Some(vertex) => {
        saveVertex(vertex, compressTime)
        if (vertex.archive(archiveTime, compressing, true, workerID)) {
          storage.vertices(workerID) //TODO nice way to delete when reenabled
          storage.vertexDeletionCount(workerID) += 1
        }
        saveEdges(vertex, compressTime, archiveTime, true, true)
      } //if all old then remove the vertex
      case None => {} //do nothing
    }
    sender() ! FinishedVertexArchiving(key)
  }

  def archiveOnlyVertex(key: Int, archiveTime: Long) = {
    storage.vertices.get(key) match {
      case Some(vertex) => {
        if (vertex.archiveOnly(archiveTime, true, workerID)) {
          storage.vertices.remove(vertex.getId.toInt)
          storage.vertexDeletionCount(workerID) += 1
        }
        saveEdges(vertex, -1, archiveTime, true, true)
      } //if all old then remove the vertex
      case None => {} //do nothing
    }
    sender() ! FinishedVertexArchiving(key)
  }

  def saveEdges(vertex: Vertex, compressTime: Long, archiveTime: Long, saving: Boolean, archiving: Boolean) = {
    for ((id, edge) <- vertex.outgoingEdges)
      edgeHelper(saving, archiving, edge, vertex, compressTime, archiveTime)
    for ((id, edge) <- vertex.incomingEdges)
      if (edge.isInstanceOf[RemoteEdge])
        edgeHelper(saving, archiving, edge, vertex, compressTime, archiveTime)
  }

  private def edgeHelper(saving: Boolean, archiving: Boolean, edge: Edge, vertex: Vertex, compressTime: Long, archiveTime: Long): Unit = {
    if (saving)
      saveEdge(edge, compressTime)
    if (archiving && saving) {
      if (edge.archive(archiveTime, compressing, false, workerID)) {
        vertex.outgoingEdges(edge.getId) = null //TODO nice way to delete when reenabled -- will probably change this func to filter
        storage.edgeDeletionCount(workerID) += 1
      }
    }
    else if(archiving && !saving)
      if (edge.archiveOnly(archiveTime,false,workerID)) {
        vertex.outgoingEdges(edge.getId) = null //TODO nice way to delete when reenabled
        storage.edgeDeletionCount(workerID)+=1
      }
  }
  private def saveEdge(edge: Edge, cutOff: Long) = {
    val history = edge.compressHistory(cutOff)
    if (saving) {
      if (history.size > 0)
        RaphtoryDBWrite.edgeHistory.save(edge.getSrcId, edge.getDstId, history)
      edge.properties.foreach(property => {
        val propHistory = property._2.compressHistory(cutOff, true, workerID)
        if (propHistory.size > 0)
          try RaphtoryDBWrite.edgePropertyHistory.save(edge.getSrcId, edge.getDstId, property._1, propHistory)
          catch {
            case e: WriteTimeoutException => {
              println("saving error")
            }
          }
      })
    }
  }

  def saveVertex(vertex: Vertex, cutOff: Long) = {
    val history = vertex.compressHistory(cutOff)
    if (saving) { //if we are saving data to cassandra
      if (history.size > 0) {
        RaphtoryDBWrite.vertexHistory.save(vertex.getId, history)
      }
      vertex.outgoingEdges.foreach(e => saveEdge(e._2, cutOff))
      vertex.incomingEdges.foreach(e => if (e._2.isInstanceOf[RemoteEdge]) saveEdge(e._2, cutOff))
      vertex.properties.foreach(prop => {
        val propHistory = prop._2.compressHistory(cutOff, false, workerID)
        if (propHistory.size > 0)
          try RaphtoryDBWrite.vertexPropertyHistory.save(vertex.getId, prop._1, propHistory)
          catch {
            case e: WriteTimeoutException => {
              println("saving error")
            }
          }
      })
    }
  }
}
/*************************************************************
  *                   ARCHIVING SECTION                      *
  ************************************************************/


