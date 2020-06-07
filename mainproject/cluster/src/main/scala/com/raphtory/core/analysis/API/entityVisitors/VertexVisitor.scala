package com.raphtory.core.analysis.API.entityVisitors

import akka.actor.{ActorContext, ActorRef}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.analysis.API.GraphLenses.GraphLens
import com.raphtory.core.analysis.API.ManagerCount
import com.raphtory.core.components.PartitionManager.Workers.ViewJob
import com.raphtory.core.model.communication._
import com.raphtory.core.model.graphentities.{Edge, MutableProperty, Vertex}
import com.raphtory.core.utils.Utils

import scala.collection.mutable
import scala.collection.parallel.ParSet
import scala.collection.parallel.mutable.ParTrieMap
object VertexVisitor {
  def apply(v: Vertex, jobID: ViewJob, superStep: Int, proxy: GraphLens)(
      implicit context: ActorContext,
      managerCount: ManagerCount
  ) =
    new VertexVisitor(v, jobID, superStep, proxy)
}
class VertexVisitor(v: Vertex, viewJob:ViewJob, superStep: Int, proxy: GraphLens)(implicit context: ActorContext, managerCount: ManagerCount) {
  val jobID = viewJob.jobID
  val timestamp = viewJob.timestamp
  val window = viewJob.window

  private val mediator: ActorRef = DistributedPubSub(context.system).mediator // get the mediator for sending cluster messages
  val vert: Vertex               = v
  def messageQueue     = v.multiQueue.getMessageQueue(viewJob, superStep)
  def vertexType                 = v.getType
  def clearQueue                   = v.multiQueue.clearQueue(viewJob, superStep)
  def getOutgoingNeighbors: ParTrieMap[Long, Edge] = v.outgoingProcessing
  def getOutgoingNeighborsAfter(time:Long):ParTrieMap[Long,EdgeVisitor] = v.outgoingProcessing.filter(e=> e._2.previousState.exists(k => k._1 >= time)).map(x=>(x._1,new EdgeVisitor(x._2)))
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

  //Send message
  def messageNeighbour(vertexID: Long, data: Any): Unit = {
    val message = VertexMessage(vertexID, viewJob, superStep, data)
    proxy.recordMessage(v.getId, vertexID, data)
    mediator ! DistributedPubSubMediator.Send(Utils.getReader(vertexID, managerCount.count), message, false)
  }

  def messageAllOutgoingNeighbors(message: Any): Unit =
    v.outgoingProcessing.foreach(vID => messageNeighbour(vID._1.toInt, message))

  def messageAllNeighbours(message: Any) =
    v.outgoingProcessing.keySet.union(v.incomingProcessing.keySet).foreach(vID => messageNeighbour(vID.toInt, message))

  def messageAllIngoingNeighbors(message: Any): Unit =
    v.incomingProcessing.foreach(vID => messageNeighbour(vID._1.toInt, message))


  def moreMessages(): Boolean = messageQueue.nonEmpty

  def voteToHalt() = proxy.vertexVoted()
  def aliveAt(time: Long): Boolean                         = v.aliveAt(time)
  def aliveAtWithWindow(time: Long, window: Long): Boolean = v.aliveAtWithWindow(time, window)

}

