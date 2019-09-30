package com.raphtory.core.components.PartitionManager.Workers

import akka.actor.{Actor, ActorRef}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.datastax.driver.core.exceptions.WriteTimeoutException
import com.raphtory.core.model.communication._
import com.raphtory.core.model.graphentities.{Edge, RemoteEdge, Vertex}
import com.raphtory.core.storage.{EntityStorage, RaphtoryDBWrite}
import com.raphtory.core.utils.Utils

class IngestionWorker(workerID:Int) extends Actor {
  val mediator              : ActorRef = DistributedPubSub(context.system).mediator // get the mediator for sending cluster messages
  mediator ! DistributedPubSubMediator.Put(self)
  val compressing    : Boolean =  Utils.compressing
  val saving    : Boolean =  Utils.saving

  override def receive:Receive = {
    case VertexAdd(routerID,msgTime,srcId) =>
      EntityStorage.vertexAdd(routerID,workerID,msgTime,srcId)
      vHandle(srcId,msgTime)
    case VertexRemoval(routerID,msgTime,srcId) =>
      EntityStorage.vertexRemoval(routerID,workerID,msgTime,srcId)
      vHandle(srcId,msgTime)
    case VertexAddWithProperties(routerID,msgTime,srcId,properties) =>
      EntityStorage.vertexAdd(routerID,workerID,msgTime,srcId,properties)
      vHandle(srcId,msgTime)

    case DstAddForOtherWorker(routerID,msgTime,dstID,srcForEdge,edge,present) =>
      EntityStorage.vertexWorkerRequest(routerID,workerID,msgTime,dstID,srcForEdge,edge,present)
      wHandle()
    case DstWipeForOtherWorker(routerID,msgTime,dstID,srcForEdge,edge,present) =>
      EntityStorage.vertexWipeWorkerRequest(routerID,workerID,msgTime,dstID,srcForEdge,edge,present)
      wHandle()
    case DstResponseFromOtherWorker(routerID,msgTime,srcForEdge,dstID,removeList) =>
      EntityStorage.vertexWorkerRequestEdgeHandler(routerID,workerID,msgTime,srcForEdge,dstID,removeList)
      wHandle()
    case EdgeRemoveForOtherWorker(routerID,msgTime,srcID,dstID) =>
      EntityStorage.edgeRemovalFromOtherWorker(routerID,workerID,msgTime,srcID,dstID)
      wHandle()
    //case EdgeRemovalAfterArchiving(routerID,msgTime,srcID,dstID)           => EntityStorage.edgeRemovalAfterArchiving(routerID,workerID,msgTime,srcID,dstID) //disabled at the moment

    case EdgeAdd(routerID,msgTime,srcId,dstId) =>
      EntityStorage.edgeAdd(routerID,workerID,msgTime,srcId,dstId)
      eHandle(srcId,dstId,msgTime)
    case EdgeAddWithProperties(routerID,msgTime,srcId,dstId,properties) =>
      EntityStorage.edgeAdd(routerID,workerID,msgTime,srcId,dstId,properties)
      eHandle(srcId,dstId,msgTime)

    case RemoteEdgeAdd(routerID,msgTime,srcId,dstId,properties) =>
      EntityStorage.remoteEdgeAdd(routerID,workerID,msgTime,srcId,dstId,properties)
      eHandleSecondary(srcId,dstId,msgTime)
    case RemoteEdgeAddNew(routerID,msgTime,srcId,dstId,properties,deaths) =>
      EntityStorage.remoteEdgeAddNew(routerID,workerID,msgTime,srcId,dstId,properties,deaths)
      eHandleSecondary(srcId,dstId,msgTime)

    case EdgeRemoval(routerID,msgTime,srcId,dstId) =>
      EntityStorage.edgeRemoval(routerID,workerID,msgTime,srcId,dstId)
      eHandle(srcId,dstId,msgTime)
    case RemoteEdgeRemoval(routerID,msgTime,srcId,dstId) =>
      EntityStorage.remoteEdgeRemoval(routerID,workerID,msgTime,srcId,dstId)
      eHandleSecondary(srcId,dstId,msgTime)
    case RemoteEdgeRemovalNew(routerID,msgTime,srcId,dstId,deaths) =>
      EntityStorage.remoteEdgeRemovalNew(routerID,workerID,msgTime,srcId,dstId,deaths)
      eHandleSecondary(srcId,dstId,msgTime)

    case ReturnEdgeRemoval(routerID,msgTime,srcId,dstId) =>
      EntityStorage.returnEdgeRemoval(routerID,workerID,msgTime,srcId,dstId)
      eHandleSecondary(srcId,dstId,msgTime)
    case RemoteReturnDeaths(routerID,msgTime,srcId,dstId,deaths) =>
      EntityStorage.remoteReturnDeaths(routerID,workerID,msgTime,srcId,dstId,deaths)
      eHandleSecondary(srcId,dstId,msgTime)

    case CompressVertex(id,time)                                          => compressVertex(id,time)
    case ArchiveVertex(id,compressTime,archiveTime)                       => archiveVertex(id,compressTime,archiveTime)

    case ArchiveOnlyEdge(id,archiveTime)                                  => archiveOnlyEdge(id,archiveTime)
    case ArchiveOnlyVertex(id,archiveTime)                                => archiveOnlyVertex(id,archiveTime)
  }

  /*************************************************************
    *                 LOG HANDLING SECTION                     *
    ************************************************************/

  def vHandle(srcID : Int,msgTime:Long) : Unit = {
    EntityStorage.timings(msgTime)
    EntityStorage.messageCount(workerID)+=1
  }
  def eHandle(srcID : Int, dstID : Int,msgTime:Long) : Unit = {
    EntityStorage.timings(msgTime)
    EntityStorage.messageCount(workerID)+=1
  }
  def eHandleSecondary(srcID : Int, dstID : Int,msgTime:Long) : Unit = {
    EntityStorage.timings(msgTime)
    EntityStorage.secondaryMessageCount(workerID)+=1
  }
  def wHandle(): Unit ={
    EntityStorage.workerMessageCount(workerID)+=1
  }

  /*************************************************************
    *                   COMPRESSION SECTION                    *
    ************************************************************/

  def compressVertex(key: Int, now: Long) = {
    EntityStorage.vertices(workerID).get(key) match {
      case Some(vertex) => saveVertex(vertex, now)
      case None =>
    }
    sender() ! FinishedVertexCompression(key)
  }

  //TODO decide what to do with placeholders (future)*
  def archiveEdge(key:Long, compressTime:Long,archiveTime:Long) = {
    EntityStorage.edges.get(key) match {
      case Some(edge) => {
        saveEdge(edge, compressTime)
        if (edge.archive(archiveTime,compressing,false,workerID)) {
          EntityStorage.edges.remove(edge.getId)
          EntityStorage.edgeDeletionCount(workerID)+=1
        }
      }//if all old then remove the edge
      case None => {}//do nothing
    }
    sender() ! FinishedEdgeArchiving(key)
  }

  def archiveVertex(key:Int,compressTime:Long,archiveTime:Long) = {
    EntityStorage.vertices(workerID).get(key) match {
      case Some(vertex) => {
        saveVertex(vertex,compressTime)
        if (vertex.archive(archiveTime,compressing,true,workerID)) {
          EntityStorage.vertices.remove(vertex.getId.toInt)
          EntityStorage.vertexDeletionCount(workerID)+=1
        }
      } //if all old then remove the vertex
      case None => {}//do nothing
    }
    sender() ! FinishedVertexArchiving(key)
  }

  def saveEdge(edge: Edge, cutOff: Long) = {
    val history = edge.compressHistory(cutOff)
    if (saving) {
      if (history.size > 0) {
        RaphtoryDBWrite.edgeHistory.save(edge.getSrcId, edge.getDstId, history)
      }
      edge.properties.foreach(property => {
        val propHistory = property._2.compressHistory(cutOff,true,workerID)
        if (propHistory.size > 0) {
          try{
            RaphtoryDBWrite.edgePropertyHistory.save(edge.getSrcId, edge.getDstId, property._1, propHistory)
          }
          catch {
            case e:WriteTimeoutException => {println("saving error")}
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
      vertex.outgoingEdges.foreach(e=> saveEdge(e._2, cutOff))
      vertex.incomingEdges.foreach(e=> if(e._2.isInstanceOf[RemoteEdge])saveEdge(e._2, cutOff))
      vertex.properties.foreach(prop => {
        val propHistory = prop._2.compressHistory(cutOff,false,workerID)
        if (propHistory.size > 0) {
          try{
          RaphtoryDBWrite.vertexPropertyHistory.save(vertex.getId, prop._1, propHistory)
          }
          catch {
            case e:WriteTimeoutException => {println("saving error")}
          }
        }
      })
    }
  }


  //TODO decide what to do with placeholders (future)*
  def archiveOnlyEdge(key:Long, archiveTime:Long) = {
    EntityStorage.edges.get(key) match {
      case Some(edge) => {
        if (edge.archiveOnly(archiveTime,false,workerID)) {
          EntityStorage.edges.remove(edge.getId)
          EntityStorage.edgeDeletionCount(workerID)+=1
        }
      }//if all old then remove the edge
      case None => {}//do nothing
    }
    sender() ! FinishedEdgeArchiving(key)
  }

  def archiveOnlyVertex(key:Int,archiveTime:Long) = {
    EntityStorage.vertices(workerID).get(key) match {
      case Some(vertex) => {
        if (vertex.archiveOnly(archiveTime,true,workerID)) {
          EntityStorage.vertices.remove(vertex.getId.toInt)
          EntityStorage.vertexDeletionCount(workerID)+=1
        }
      } //if all old then remove the vertex
      case None => {}//do nothing
    }
    sender() ! FinishedVertexArchiving(key)
  }


}
  /*************************************************************
    *                   ARCHIVING SECTION                      *
    ************************************************************/


