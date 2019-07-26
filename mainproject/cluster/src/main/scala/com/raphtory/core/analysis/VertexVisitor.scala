package com.raphtory.core.analysis
import akka.actor.{ActorContext, ActorRef}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.model.communication.{EdgeUpdateProperty, MessageHandler, VertexMessage}
import com.raphtory.core.model.graphentities.{Edge, Property, Vertex}
import com.raphtory.core.storage.EntityStorage
import com.raphtory.core.utils.Utils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ParSet
import scala.collection.parallel.mutable.ParArray
object VertexVisitor  {
  def apply(v : Vertex,jobID:String,superStep:Int,proxy:GraphRepoProxy)(implicit context : ActorContext, managerCount : ManagerCount) = {
    new VertexVisitor(v,jobID,superStep,proxy)
  }
}
class VertexVisitor(v : Vertex,jobID:String,superStep:Int,proxy:GraphRepoProxy)(implicit context : ActorContext, managerCount : ManagerCount) {

  private val mediator : ActorRef   = DistributedPubSub(context.system).mediator // get the mediator for sending cluster messages
  val vert:Vertex = v
  val messageQueue = v.vertexMultiQueue.getMessageQueue(jobID,superStep)
  val messageQueue2 = v.vertexMultiQueue.getMessageQueue(jobID,superStep+1)
  def getOutgoingNeighbors : ParArray[Int] = v.outgoingIDs.toParArray
  def getIngoingNeighbors  : ParArray[Int] = v.incomingIDs.toParArray
  def getAllNeighbors: ParArray[Int] = v.incomingIDs.union(v.outgoingIDs).toParArray

  def getOutgoingNeighborProp(vId: Int, key : String) : Option[String] = {
    EntityStorage.edges.get(Utils.getEdgeIndex(v.vertexId,vId)) match {
      case Some(e) => e.getPropertyCurrentValue(key)
      case None    => None
    }
  }
  def getIngoingNeighborProp(vId : Int, key : String) : Option[String] = {
    EntityStorage.edges.get(Utils.getEdgeIndex(vId,v.vertexId)) match {
      case Some(e) => e.getPropertyCurrentValue(key)
      case None    => None
    }
  }

  def getNeighborsProp(key : String) : ArrayBuffer[String] = {
    var values = mutable.ArrayBuffer[String]()
    v.incomingEdges.foreach(e => {
      values += e._2.getPropertyCurrentValue(key).get
    })
    v.outgoingEdges.foreach(e => {
      values += e._2.getPropertyCurrentValue(key).get
    })
    values
  }

  def getPropertySet():ParSet[String] = {
    v.properties.keySet
  }
  def getPropertyCurrentValue(key : String) : Option[String] =
    v.properties.get(key) match {
      case Some(p) => Some(p.currentValue)
      case None => None
    }

  def setCompValue(key:String,value:Any):Unit = {
    v.addCompValue(key,value)
  }
  def getCompValue(key:String) = {
    v.getCompValue(key)
  }
  def getOrSetCompValue(key:String,value:Any) ={
    v.getOrSet(key,value)
  }

  def messageNeighbour(vertexID : Int, message:VertexMessage) : Unit = {
    proxy.recordMessage()
    mediator ! DistributedPubSubMediator.Send(Utils.getReader(vertexID, managerCount.count),MessageHandler(vertexID,jobID,superStep,message),false)
  }

  def messageAllOutgoingNeighbors(message: VertexMessage) : Unit = v.outgoingIDs.foreach(vID => messageNeighbour(vID,message))

  def messageAllNeighbours(message:VertexMessage) = v.outgoingIDs.union(v.incomingIDs).foreach(vID => messageNeighbour(vID,message))

  def messageAllIngoingNeighbors(message: VertexMessage) : Unit = v.incomingIDs.foreach(vID => messageNeighbour(vID,message))

  def moreMessages():Boolean = messageQueue.nonEmpty
  def nextMessage():VertexMessage = messageQueue.pop()

  def voteToHalt() = {
    proxy.vertexVoted()
  }

//  private def edgeFilter(srcId: Int, dstId: Int, edgeId : Long) : Boolean = Utils.getEdgeIndex(srcId, dstId) == edgeId
//  private def outgoingEdgeFilter(dstId : Int, edgeId : Long) : Boolean = edgeFilter(v.getId.toInt, dstId, edgeId)
//  private def ingoingEdgeFilter(srcId : Int, edgeId : Long) : Boolean = edgeFilter(srcId, v.getId.toInt, edgeId)
  //private def getNeighbor(f: Long => Boolean) : Option[Edge] = v.associatedEdges.values.find(e => f(e.getId))
  //private def getOutgoingNeighbor(vId : Int) = getNeighbor(e => outgoingEdgeFilter(vId, e))
  //private def getIngoingNeighbor(vId : Int) = getNeighbor(e => ingoingEdgeFilter(vId, e))



  //private def getNeighbors = v.associatedEdges
}

//def getOutgoingNeighbors : ParArray[Int] = v.outgoingEdges.values.map(e => e.getDstId).toParArray
//def getIngoingNeighbors  : ParArray[Int] = v.incomingEdges.values.map(e => e.getSrcId).toParArray

//  def getOutgoingNeighborProp(vId: Int, key : String) : Option[String] = {
//    v.outgoingEdges.get(Utils.getEdgeIndex(v.vertexId,vId)) match {
//      case Some(e) => e.getPropertyCurrentValue(key)
//      case None    => None
//    }
//  }
//  def getIngoingNeighborProp(vId : Int, key : String) : Option[String] = {
//    v.incomingEdges.get(Utils.getEdgeIndex(vId,v.vertexId)) match {
//      case Some(e) => e.getPropertyCurrentValue(key)
//      case None    => None
//    }
//  }

//  def updateProperty(key : String, value : String) = {
//    v.synchronized {
//      v.properties.get(key) match {
//        case None =>
//          v.properties.put(key, new Property(System.currentTimeMillis(), key, value))
//        case Some(oldProp) => oldProp.update(System.currentTimeMillis(), value)
//      }
//    }
//  }