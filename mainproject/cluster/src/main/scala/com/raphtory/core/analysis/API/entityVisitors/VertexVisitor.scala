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
import scala.collection.parallel.mutable.{ParIterable, ParTrieMap}
import scala.reflect.ClassTag
object VertexVisitor {
  def apply(v: Vertex, jobID: ViewJob, superStep: Int, proxy: GraphLens)(implicit context: ActorContext, managerCount: ManagerCount) =
    new VertexVisitor(v, jobID, superStep, proxy)
}
class VertexVisitor(v: Vertex, viewJob:ViewJob, superStep: Int, view: GraphLens)(implicit context: ActorContext, managerCount: ManagerCount) extends EntityVisitor(v,viewJob:ViewJob){
  val jobID = viewJob.jobID
  val timestamp = viewJob.timestamp
  val window = viewJob.window

  private val mediator: ActorRef = DistributedPubSub(context.system).mediator // get the mediator for sending cluster messages

  def ID() = v.vertexId
  def Type() = v.getType

  def messageQueue[T: ClassTag]  = { //clears queue after getting it to make sure not there for next iteration
    val queue = v.multiQueue.getMessageQueue(viewJob, superStep)map(_.asInstanceOf[T])
    v.multiQueue.clearQueue(viewJob, superStep)
    queue
  }

  def clearQueue = v.multiQueue.clearQueue(viewJob, superStep)

  def voteToHalt() = view.vertexVoted()
  def aliveAt(time: Long): Boolean                         = v.aliveAt(time)
  def aliveAtWithWindow(time: Long, window: Long): Boolean = v.aliveAtWithWindow(time, window)


  //out edges whole
  def getOutEdges: ParIterable[EdgeVisitor] = v.outgoingProcessing.map(e=> visitify(e._2,e._1))
  def getOutEdgesAfter(time:Long):ParIterable[EdgeVisitor] = v.outgoingProcessing.filter(e=> e._2.activityAfter(time)).map(e=> visitify(e._2,e._1))
  def getOutEdgesBefore(time:Long):ParIterable[EdgeVisitor] = v.outgoingProcessing.filter(e=> e._2.activityBefore(time)).map(e=> visitify(e._2,e._1))
  def getOutEdgesBetween(min:Long, max:Long):ParIterable[EdgeVisitor] = v.outgoingProcessing.filter(e=> e._2.activityBetween(min,max)).map(e=> visitify(e._2,e._1))

  //in edges whole
  def getIncEdges: ParIterable[EdgeVisitor]  = v.incomingProcessing.map(e=> visitify(e._2,e._1))
  def getIncEdgesAfter(time:Long):ParIterable[EdgeVisitor] = v.incomingProcessing.filter(e=> e._2.activityAfter(time)).map(e=> visitify(e._2,e._1))
  def getInCEdgesBefore(time:Long):ParIterable[EdgeVisitor] = v.incomingProcessing.filter(e=> e._2.activityBefore(time)).map(e=> visitify(e._2,e._1))
  def getInCEdgesBetween(min:Long, max:Long):ParIterable[EdgeVisitor] = v.incomingProcessing.filter(e=> e._2.activityBetween(min,max)).map(e=> visitify(e._2,e._1))


  //out edges individual
  def getOutEdge(id:Long): Option[EdgeVisitor] = v.outgoingProcessing.get(id) match {
    case(e:Some[Edge]) =>  Some(visitify(e.get,id))
    case(None) => None
  }

  def getOutEdgeAfter(id:Long,time:Long): Option[EdgeVisitor] = v.outgoingProcessing.get(id) match {
    case(e:Some[Edge]) =>
      if(e.get.activityAfter(time)) Some(visitify(e.get,id))
      else None
    case(None) => None
  }

  def getOutEdgeBefore(id:Long,time:Long): Option[EdgeVisitor] = v.outgoingProcessing.get(id) match {
    case(e:Some[Edge]) =>
      if(e.get.activityBefore(time)) Some(visitify(e.get,id))
      else None
    case(None) => None
  }

  def getOutEdgeBetween(id:Long,min:Long,max:Long): Option[EdgeVisitor] = v.outgoingProcessing.get(id) match {
    case(e:Some[Edge]) =>
      if(e.get.activityBetween(min,max)) Some(visitify(e.get,id))
      else None
    case(None) => None
  }

  //In edges individual
  def getInEdge(id:Long): Option[EdgeVisitor] = v.incomingProcessing.get(id) match {
    case(e:Some[Edge]) =>  Some(visitify(e.get,id))
    case(None) => None
  }

  def getInEdgeAfter(id:Long,time:Long): Option[EdgeVisitor] = v.incomingProcessing.get(id) match {
    case(e:Some[Edge]) =>
      if(e.get.activityAfter(time)) Some(visitify(e.get,id))
      else None
    case(None) => None
  }

  def getInEdgeBefore(id:Long,time:Long): Option[EdgeVisitor] = v.incomingProcessing.get(id) match {
    case(e:Some[Edge]) =>
      if(e.get.activityBefore(time)) Some(visitify(e.get,id))
      else None
    case(None) => None
  }

  def getInEdgeBetween(id:Long,min:Long,max:Long): Option[EdgeVisitor] = v.incomingProcessing.get(id) match {
    case(e:Some[Edge]) =>
      if(e.get.activityBetween(min,max)) Some(visitify(e.get,id))
      else None
    case(None) => None
  }



  //TODO work on properties
  def getPropertySet(): ParSet[String] = v.properties.keySet

  def getPropertyValue(key: String): Option[Any] =
    v.properties.get(key) match {
      case Some(p) => Some(p.currentValue)
      case None    => None
    }

  def setState(key: String, value: Any): Unit = {
    val realkey = key + timestamp + window
    v.addCompValue(realkey, value)
  }
  def getState[T: ClassTag](key: String) = {
    val realkey = key + timestamp + window
    v.getCompValue(realkey).asInstanceOf[T]
  }
  def containsState(key: String): Boolean = {
    val realkey = key + timestamp + window
    v.containsCompvalue(realkey)
  }
  def getOrSetState[T: ClassTag](key: String, value: Any) = {
    val realkey = key + timestamp + window
    v.getOrSet(realkey, value).asInstanceOf[T]
  }

  def appendToState[T: ClassTag](key: String, value: Any) = { //write function later
    val realkey = key + timestamp + window
    if(v.containsCompvalue(realkey))
      v.addCompValue(realkey,v.getCompValue(realkey).asInstanceOf[Array[Any]] ++ Array(value))
    else
      v.addCompValue(realkey,Array(value))
  }

  //Send message
  def messageNeighbour(vertexID: Long, data: Any): Unit = {
    val message = VertexMessage(vertexID, viewJob, superStep, data)
    view.recordMessage()
    mediator ! DistributedPubSubMediator.Send(Utils.getReader(vertexID, managerCount.count), message, false)
  }

  def messageAllOutgoingNeighbors(message: Any): Unit =
    v.outgoingProcessing.foreach(vID => messageNeighbour(vID._1.toInt, message))

  def messageAllNeighbours(message: Any) =
    v.outgoingProcessing.keySet.union(v.incomingProcessing.keySet).foreach(vID => messageNeighbour(vID.toInt, message))

  def messageAllIngoingNeighbors(message: Any): Unit =
    v.incomingProcessing.foreach(vID => messageNeighbour(vID._1.toInt, message))



  def visitify(edge:Edge,id:Long) = new EdgeVisitor(edge,id,viewJob,superStep,view,mediator)

}

