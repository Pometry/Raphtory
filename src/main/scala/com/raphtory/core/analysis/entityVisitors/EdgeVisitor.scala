package com.raphtory.core.analysis.entityVisitors

import akka.actor.{ActorContext, ActorRef}
import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.analysis.api.ManagerCount
import com.raphtory.core.actors.PartitionManager.Workers.ViewJob
import com.raphtory.core.analysis.GraphLenses.GraphLens
import com.raphtory.core.model.communication.VertexMessage
import com.raphtory.core.model.graphentities.{Edge, ImmutableProperty, MutableProperty}

import scala.collection.mutable

class EdgeVisitor(edge:Edge,id:Long,viewJob:ViewJob,superStep:Int,view:GraphLens,mediator: ActorRef)(implicit context: ActorContext, managerCount: ManagerCount) extends EntityVisitor(edge,viewJob:ViewJob) {


  def ID() = id
  def src() = edge.getSrcId
  def dst() = edge.getDstId


  def send(data: Any): Unit = {
    val message = VertexMessage(id, viewJob, superStep, data)
    view.recordMessage()
    mediator ! DistributedPubSubMediator.Send(getReader(id, managerCount.count), message, false)
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

}
