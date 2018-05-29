package com.raphtory.core.analysis
import akka.actor.{ActorContext, ActorRef}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.model.communication.EdgeUpdateProperty
import com.raphtory.core.model.graphentities.{Edge, Property, Vertex}
import com.raphtory.core.utils.Utils
object VertexVisitor  {
  def apply(v : Vertex)(implicit context : ActorContext, managerCount : Int) = {
    new VertexVisitor(v)
  }
}
class VertexVisitor(v : Vertex)(implicit context : ActorContext, managerCount : Int) {

  private val mediator : ActorRef   = DistributedPubSub(context.system).mediator // get the mediator for sending cluster messages
  def getOutgoingNeighborProp(vId: Int, key : String) : Option[String] = getNeighborProp(getOutgoingNeighbor(vId), key)
  def getIngoingNeighborProp(vId : Int, key : String) : Option[String] = getNeighborProp(getIngoingNeighbor(vId), key)

  def getNeighborsProp(key : String) : Vector[String] = {
    var values = Vector.empty[String]
    getNeighbors.foreach(e => {
      values :+= e.getPropertyCurrentValue(key).get
    })
    values
  }

  private def pushToNeighbor(edgeId : Long, key: String, value : String) : Unit = {
    mediator ! DistributedPubSubMediator.Send(Utils.getManager(Utils.getIndexHI(edgeId), managerCount),
      EdgeUpdateProperty(System.currentTimeMillis(), edgeId, key, value), false)

  }

  def getOutgoingNeighbors : Vector[Int] =
    v.associatedEdges.filter(e => e.getSrcId == v.getId).map(e => e.getDstId).toVector


  def getIngoingNeighbors  : Vector[Int] =
    v.associatedEdges.filter(e => e.getDstId == v.getId).map(e => e.getSrcId).toVector

  def getPropertyCurrentValue(key : String) : Option[String] =
    v.properties.get(key) match {
      case Some(p) => Some(p.currentValue)
      case None => None
    }

  def updateProperty(key : String, value : String) = {
    v.properties.get(key) match {
      case None =>
        v.properties.put (key, new Property (System.currentTimeMillis (), key, value) )
      case Some (oldProp) => oldProp.update (System.currentTimeMillis (), value)
    }
  }

  def pushToOutgoingNeighbor(dstId : Int, key : String, value : String) : Unit =
    pushToNeighbor(Utils.getEdgeIndex(v.getId.toInt, dstId), key, value)

  def pushToIngoingNeighbor(srcId : Int, key : String, value : String) : Unit =
    pushToNeighbor(Utils.getEdgeIndex(srcId, v.getId.toInt), key, value)

  private def edgeFilter(srcId: Int, dstId: Int, edgeId : Long) : Boolean = Utils.getEdgeIndex(srcId, dstId) == edgeId
  private def outgoingEdgeFilter(dstId : Int, edgeId : Long) : Boolean = edgeFilter(v.getId.toInt, dstId, edgeId)
  private def ingoingEdgeFilter(srcId : Int, edgeId : Long) : Boolean = edgeFilter(srcId, v.getId.toInt, edgeId)

  private def getNeighbor(f: Long => Boolean) : Option[Edge] = v.associatedEdges.find(e => f(e.getId))
  private def getOutgoingNeighbor(vId : Int) = getNeighbor(e => outgoingEdgeFilter(vId, e))
  private def getIngoingNeighbor(vId : Int) = getNeighbor(e => ingoingEdgeFilter(vId, e))

  private def getNeighborProp(f : => Option[Edge], key : String) = {
    f match {
      case Some(e) => e.getPropertyCurrentValue(key)
      case None    => None
    }
  }

  private def getNeighbors = v.associatedEdges
}
