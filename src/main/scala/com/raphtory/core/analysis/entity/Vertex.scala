package com.raphtory.core.analysis.entity

import akka.actor.{ActorContext, ActorRef}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.analysis.api.ManagerCount
import com.raphtory.core.actors.PartitionManager.Workers.ViewJob
import com.raphtory.core.analysis.GraphLenses.GraphLens
import com.raphtory.core.model.communication._
import com.raphtory.core.model.entities.{RaphtoryEdge, RaphtoryVertex}

import scala.collection.parallel.mutable.ParIterable
import scala.reflect.ClassTag
object Vertex {
  def apply(v: RaphtoryVertex, jobID: ViewJob, superStep: Int, proxy: GraphLens)(implicit context: ActorContext, managerCount: ManagerCount) =
    new Vertex(v, jobID, superStep, proxy)
}
class Vertex(v: RaphtoryVertex, viewJob:ViewJob, superStep: Int, view: GraphLens)(implicit context: ActorContext, managerCount: ManagerCount) extends EntityVisitor(v,viewJob:ViewJob){
  val jobID = viewJob.jobID
  val timestamp = viewJob.timestamp
  val window = viewJob.window

  private val mediator: ActorRef = DistributedPubSub(context.system).mediator // get the mediator for sending cluster messages

  def ID() = v.vertexId


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
  def getOutEdges: ParIterable[Edge] = v.outgoingProcessing.map(e=> visitify(e._2,e._1))
  def getOutEdgesAfter(time:Long):ParIterable[Edge] = v.outgoingProcessing.filter(e=> e._2.activityAfter(time)).map(e=> visitify(e._2,e._1))
  def getOutEdgesBefore(time:Long):ParIterable[Edge] = v.outgoingProcessing.filter(e=> e._2.activityBefore(time)).map(e=> visitify(e._2,e._1))
  def getOutEdgesBetween(min:Long, max:Long):ParIterable[Edge] = v.outgoingProcessing.filter(e=> e._2.activityBetween(min,max)).map(e=> visitify(e._2,e._1))

  //in edges whole
  def getIncEdges: ParIterable[Edge]  = v.incomingProcessing.map(e=> visitify(e._2,e._1))
  def getIncEdgesAfter(time:Long):ParIterable[Edge] = v.incomingProcessing.filter(e=> e._2.activityAfter(time)).map(e=> visitify(e._2,e._1))
  def getInCEdgesBefore(time:Long):ParIterable[Edge] = v.incomingProcessing.filter(e=> e._2.activityBefore(time)).map(e=> visitify(e._2,e._1))
  def getInCEdgesBetween(min:Long, max:Long):ParIterable[Edge] = v.incomingProcessing.filter(e=> e._2.activityBetween(min,max)).map(e=> visitify(e._2,e._1))


  //out edges individual
  def getOutEdge(id:Long): Option[Edge] = v.outgoingProcessing.get(id) match {
    case(e:Some[RaphtoryEdge]) =>  Some(visitify(e.get,id))
    case(None) => None
  }

  def getOutEdgeAfter(id:Long,time:Long): Option[Edge] = v.outgoingProcessing.get(id) match {
    case(e:Some[RaphtoryEdge]) =>
      if(e.get.activityAfter(time)) Some(visitify(e.get,id))
      else None
    case(None) => None
  }

  def getOutEdgeBefore(id:Long,time:Long): Option[Edge] = v.outgoingProcessing.get(id) match {
    case(e:Some[RaphtoryEdge]) =>
      if(e.get.activityBefore(time)) Some(visitify(e.get,id))
      else None
    case(None) => None
  }

  def getOutEdgeBetween(id:Long,min:Long,max:Long): Option[Edge] = v.outgoingProcessing.get(id) match {
    case(e:Some[RaphtoryEdge]) =>
      if(e.get.activityBetween(min,max)) Some(visitify(e.get,id))
      else None
    case(None) => None
  }

  //In edges individual
  def getInEdge(id:Long): Option[Edge] = v.incomingProcessing.get(id) match {
    case(e:Some[RaphtoryEdge]) =>  Some(visitify(e.get,id))
    case(None) => None
  }

  def getInEdgeAfter(id:Long,time:Long): Option[Edge] = v.incomingProcessing.get(id) match {
    case(e:Some[RaphtoryEdge]) =>
      if(e.get.activityAfter(time)) Some(visitify(e.get,id))
      else None
    case(None) => None
  }

  def getInEdgeBefore(id:Long,time:Long): Option[Edge] = v.incomingProcessing.get(id) match {
    case(e:Some[RaphtoryEdge]) =>
      if(e.get.activityBefore(time)) Some(visitify(e.get,id))
      else None
    case(None) => None
  }

  def getInEdgeBetween(id:Long,min:Long,max:Long): Option[Edge] = v.incomingProcessing.get(id) match {
    case(e:Some[RaphtoryEdge]) =>
      if(e.get.activityBetween(min,max)) Some(visitify(e.get,id))
      else None
    case(None) => None
  }


  def setState(key: String, value: Any): Unit = {
    val realkey = key + timestamp + window + jobID
    v.addCompValue(realkey, value)
  }
  def getState[T: ClassTag](key: String) = {
    val realkey = key + timestamp + window + jobID
    v.getCompValue(realkey).asInstanceOf[T]
  }
  def containsState(key: String): Boolean = {
    val realkey = key + timestamp + window + jobID
    v.containsCompvalue(realkey)
  }
  def getOrSetState[T: ClassTag](key: String, value: Any) = {
    val realkey = key + timestamp + window + jobID
    v.getOrSet(realkey, value).asInstanceOf[T]
  }

  def appendToState[T: ClassTag](key: String, value: Any) = { //write function later
    val realkey = key + timestamp + window + jobID
    if(v.containsCompvalue(realkey))
      v.addCompValue(realkey,v.getCompValue(realkey).asInstanceOf[Array[Any]] ++ Array(value))
    else
      v.addCompValue(realkey,Array(value))
  }

  //Send message
  def messageNeighbour(vertexID: Long, data: Any): Unit = {
    val message = VertexMessage(vertexID, viewJob, superStep, data)
    view.recordMessage()
    mediator ! DistributedPubSubMediator.Send(getReader(vertexID, managerCount.count), message, false)
  }

  def messageAllOutgoingNeighbors(message: Any): Unit =
    v.outgoingProcessing.foreach(vID => messageNeighbour(vID._1.toInt, message))

  def messageAllNeighbours(message: Any) =
    v.outgoingProcessing.keySet.union(v.incomingProcessing.keySet).foreach(vID => messageNeighbour(vID.toInt, message))

  def messageAllIngoingNeighbors(message: Any): Unit =
    v.incomingProcessing.foreach(vID => messageNeighbour(vID._1.toInt, message))



  private def visitify(edge:RaphtoryEdge, id:Long) = new Edge(edge,id,viewJob,superStep,view,mediator)

}

