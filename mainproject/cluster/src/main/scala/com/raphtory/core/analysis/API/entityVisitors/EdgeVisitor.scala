package com.raphtory.core.analysis.API.entityVisitors

import akka.actor.{ActorContext, ActorRef}
import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.analysis.API.GraphLenses.GraphLens
import com.raphtory.core.analysis.API.ManagerCount
import com.raphtory.core.components.PartitionManager.Workers.ViewJob
import com.raphtory.core.model.communication.{ImmutableProperty, VertexMessage}
import com.raphtory.core.model.graphentities.{Edge, MutableProperty}
import com.raphtory.core.utils.Utils

import scala.collection.mutable

class EdgeVisitor(edge:Edge,id:Long,viewJob:ViewJob,superStep:Int,view:GraphLens,mediator: ActorRef)(implicit context: ActorContext, managerCount: ManagerCount) extends EntityVisitor(edge,viewJob:ViewJob) {


  def ID() = id
  def src() = edge.getSrcId
  def dst() = edge.getDstId


  def send(data: Any): Unit = {
    val message = VertexMessage(id, viewJob, superStep, data)
    view.recordMessage()
    mediator ! DistributedPubSubMediator.Send(Utils.getReader(id, managerCount.count), message, false)
  }

  //TODO edge properties
  private def getEdgePropertyValuesAfterTime(edge: Edge, key: String, time: Long, window: Long): Option[mutable.TreeMap[Long, Any]] =
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

 // def getOutgoingEdgePropertyValues(key: String) =
 //   v.outgoingEdges.map(e => getEdgePropertyValuesAfterTime(e._2, key, timestamp, window).get.values)
 // def getIncomingEdgePropertyValues(key: String) =
 //   v.incomingEdges.map(e => getEdgePropertyValuesAfterTime(e._2, key, timestamp, window).get.values)


}
