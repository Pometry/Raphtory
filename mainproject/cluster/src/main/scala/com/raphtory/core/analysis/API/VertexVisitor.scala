package com.raphtory.core.analysis.API

import akka.actor.ActorContext
import akka.actor.ActorRef
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.analysis.API.GraphLenses.LiveLens
import com.raphtory.core.model.communication._
import com.raphtory.core.model.graphentities.Edge
import com.raphtory.core.model.graphentities.MutableProperty
import com.raphtory.core.model.graphentities.Vertex
import com.raphtory.core.utils.Utils

import scala.collection.mutable
import scala.collection.parallel.ParSet
import scala.collection.parallel.mutable.ParTrieMap
object VertexVisitor {
  def apply(v: Vertex, jobID: String, superStep: Int, proxy: LiveLens, timestamp: Long, window: Long)(
      implicit context: ActorContext,
      managerCount: ManagerCount
  ) =
    new VertexVisitor(v, jobID, superStep, proxy, timestamp, window)
}
class VertexVisitor(v: Vertex, jobID: String, superStep: Int, proxy: LiveLens, timestamp: Long, window: Long)(
    implicit context: ActorContext,
    managerCount: ManagerCount
) {

  private val mediator: ActorRef = DistributedPubSub(context.system).mediator // get the mediator for sending cluster messages
  val vert: Vertex               = v
  def messageQueue               = v.multiQueue.getMessageQueue(jobID, superStep)
  def vertexType                 = v.getType
  def clearQueue                 = v.multiQueue.clearQueue(jobID, superStep)
  //val messageQueue2 = v.multiQueue.getMessageQueue(jobID,superStep+1)
  def getOutgoingNeighbors: ParTrieMap[Long, Edge] = v.outgoingProcessing
  def getIngoingNeighbors: ParTrieMap[Long, Edge]  = v.incomingProcessing

  //TODO fix properties
  def getOutgoingNeighborProp(ID: Long, key: String): Option[Any] =
    getOutgoingNeighbors.get(ID) match {
      case Some(e) => e.getPropertyCurrentValue(key)
      case None    => None
    }
  def getIngoingNeighborProp(ID: Long, key: String): Option[Any] =
    getIngoingNeighbors.get(ID) match {
      case Some(e) => e.getPropertyCurrentValue(key)
      case None    => None
    }

  def getPropertySet(): ParSet[String] =
    v.properties.keySet

  def getPropertyCurrentValue(key: String): Option[Any] =
    v.properties.get(key) match {
      case Some(p) => Some(p.currentValue)
      case None    => None
    }

  private def getEdgePropertyValuesAfterTime(
      edge: Edge,
      key: String,
      time: Long,
      window: Long
  ): Option[mutable.TreeMap[Long, Any]] =
    if (window == -1L)
      edge.properties.get(key) match {
        case Some(p: MutableProperty)   => Some(p.previousState.filter(x => x._1 <= time))
        case Some(p: ImmutableProperty) => Some(mutable.TreeMap[Long, Any]((-1L -> p.currentValue)))
        case None                       => None
      }
    else
      edge.properties.get(key) match {
        case Some(p: MutableProperty)   => Some(p.previousState.filter(x => x._1 <= time && time - x._1 <= window))
        case Some(p: ImmutableProperty) => Some(mutable.TreeMap[Long, Any]((-1L -> p.currentValue)))
        case None                       => None
      }

  def getOutgoingEdgePropertyValues(key: String) =
    v.outgoingEdges.map(e => getEdgePropertyValuesAfterTime(e._2, key, timestamp, window).get.values)
  def getIncomingEdgePropertyValues(key: String) =
    v.incomingEdges.map(e => getEdgePropertyValuesAfterTime(e._2, key, timestamp, window).get.values)

  def setCompValue(key: String, value: Any): Unit = {
    val realkey = key + timestamp + window
    v.addCompValue(realkey, value)
  }
  def getCompValue(key: String) = {
    val realkey = key + timestamp + window
    v.getCompValue(realkey)
  }
  def containsCompValue(key: String): Boolean = {
    val realkey = key + timestamp + window
    v.containsCompvalue(realkey)
  }
  def getOrSetCompValue(key: String, value: Any) = {
    val realkey = key + timestamp + window
    v.getOrSet(realkey, value)
  }

  //Send String
  def messageNeighbour(vertexID: Long, data: String): Unit = {
    val message = VertexMessageString(v.getId, vertexID, jobID, superStep, data)
    proxy.recordMessage(v.getId, vertexID, data)
    mediator ! DistributedPubSubMediator.Send(Utils.getReader(vertexID, managerCount.count), message, false)
  }
  def messageAllOutgoingNeighbors(message: String): Unit =
    v.outgoingProcessing.foreach(vID => messageNeighbour(vID._1.toInt, message))
  def messageAllNeighbours(message: String) =
    v.outgoingProcessing.keySet.union(v.incomingProcessing.keySet).foreach(vID => messageNeighbour(vID.toInt, message))
  def messageAllIngoingNeighbors(message: String): Unit =
    v.incomingProcessing.foreach(vID => messageNeighbour(vID._1.toInt, message))

  //Send Long
  def messageNeighbour(vertexID: Long, data: Long): Unit = {
    val message = VertexMessageLong(v.getId, vertexID, jobID, superStep, data)
    proxy.recordMessage(v.getId, vertexID, data)
    mediator ! DistributedPubSubMediator.Send(Utils.getReader(vertexID, managerCount.count), message, false)
  }
  def messageAllOutgoingNeighbors(message: Long): Unit =
    v.outgoingProcessing.foreach(vID => messageNeighbour(vID._1.toInt, message))
  def messageAllNeighbours(message: Long) =
    v.outgoingProcessing.keySet.union(v.incomingProcessing.keySet).foreach(vID => messageNeighbour(vID.toInt, message))
  def messageAllIngoingNeighbors(message: Long): Unit =
    v.incomingProcessing.foreach(vID => messageNeighbour(vID._1.toInt, message))

  //send int
  def messageNeighbour(vertexID: Long, data: Int): Unit = {
    val message = VertexMessageInt(v.getId, vertexID, jobID, superStep, data)
    proxy.recordMessage(v.getId, vertexID, data)
    mediator ! DistributedPubSubMediator.Send(Utils.getReader(vertexID, managerCount.count), message, false)
  }
  def messageAllOutgoingNeighbors(message: Int): Unit =
    v.outgoingProcessing.foreach(vID => messageNeighbour(vID._1.toInt, message))
  def messageAllNeighbours(message: Int) =
    v.outgoingProcessing.keySet.union(v.incomingProcessing.keySet).foreach(vID => messageNeighbour(vID.toInt, message))
  def messageAllIngoingNeighbors(message: Int): Unit =
    v.incomingProcessing.foreach(vID => messageNeighbour(vID._1.toInt, message))

  //send float
  def messageNeighbour(vertexID: Long, data: Float): Unit = {
    val message = VertexMessageFloat(v.getId, vertexID, jobID, superStep, data)
    proxy.recordMessage(v.getId, vertexID, data)
    mediator ! DistributedPubSubMediator.Send(Utils.getReader(vertexID, managerCount.count), message, false)
  }
  def messageAllOutgoingNeighbors(message: Float): Unit =
    v.outgoingProcessing.foreach(vID => messageNeighbour(vID._1.toInt, message))
  def messageAllNeighbours(message: Float) =
    v.outgoingProcessing.keySet.union(v.incomingProcessing.keySet).foreach(vID => messageNeighbour(vID.toInt, message))
  def messageAllIngoingNeighbors(message: Float): Unit =
    v.incomingProcessing.foreach(vID => messageNeighbour(vID._1.toInt, message))

  def moreMessages(): Boolean = messageQueue.nonEmpty

  def voteToHalt() =
    proxy.vertexVoted()
  def aliveAt(time: Long): Boolean                         = v.aliveAt(time)
  def aliveAtWithWindow(time: Long, window: Long): Boolean = v.aliveAtWithWindow(time, window)
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
