package com.raphtory.core.analysis
import akka.actor.{ActorContext, ActorRef}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.model.communication.EdgeUpdateProperty
import com.raphtory.core.model.graphentities.{Edge, Property, Vertex}
import com.raphtory.core.storage.EntityStorage
import com.raphtory.core.utils.Utils

import scala.collection.parallel.ParSet
import scala.collection.parallel.mutable.ParArray
object VertexVisitor  {
  def apply(v : Vertex)(implicit context : ActorContext, managerCount : Int) = {
    new VertexVisitor(v)
  }
}
class VertexVisitor(v : Vertex)(implicit context : ActorContext, managerCount : Int) {

  private val mediator : ActorRef   = DistributedPubSub(context.system).mediator // get the mediator for sending cluster messages

  def getOutgoingNeighbors : ParArray[Int] = v.outgoingIDs.toParArray
  def getIngoingNeighbors  : ParArray[Int] = v.incomingIDs.toParArray

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

  def getNeighborsProp(key : String) : Vector[String] = {
    var values = Vector.empty[String]
    v.incomingEdges.foreach(e => {
      values :+= e._2.getPropertyCurrentValue(key).get
    })
    v.outgoingEdges.foreach(e => {
      values :+= e._2.getPropertyCurrentValue(key).get
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

  def updateProperty(key : String, value : String) = {
    v.synchronized {
      v.properties.get(key) match {
        case None =>
          v.properties.put(key, new Property(System.currentTimeMillis(), key, value))
        case Some(oldProp) => oldProp.update(System.currentTimeMillis(), value)
      }
    }
  }


  def pushToOutgoingNeighbor(dstId : Int, key : String, value : String) : Unit =
    pushToNeighbor(Utils.getEdgeIndex(v.getId.toInt, dstId), key, value)

  def pushToIngoingNeighbor(srcId : Int, key : String, value : String) : Unit =
    pushToNeighbor(Utils.getEdgeIndex(srcId, v.getId.toInt), key, value)

  private def pushToNeighbor(edgeId : Long, key: String, value : String) : Unit = {
    mediator ! DistributedPubSubMediator.Send(Utils.getManager(Utils.getIndexHI(edgeId), managerCount),
      EdgeUpdateProperty(System.currentTimeMillis(), edgeId, key, value), false)

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