package com.raphtory.core.analysis.entity

import akka.actor.{ActorContext, ActorRef}
import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.analysis.api.ManagerCount
import com.raphtory.core.actors.PartitionManager.Workers.ViewJob
import com.raphtory.core.analysis.GraphLens
import com.raphtory.core.model.communication.VertexMessage
import com.raphtory.core.model.entities.{ImmutableProperty, MutableProperty, RaphtoryEdge}

import scala.collection.mutable

final case class Edge(edge:RaphtoryEdge, id:Long, view: GraphLens) extends EntityVisitor(edge,view) {
  def ID() = id
  def src() = edge.getSrcId
  def dst() = edge.getDstId

  def send(data: Any): Unit =
    view.sendMessage(VertexMessage(id, data))

  def activityAfter(time:Long): Boolean = edge.history.exists(k => k._1 >= time)
  def activityBefore(time:Long): Boolean = edge.history.exists(k => k._1 <= time)
  def activityBetween(min:Long, max:Long): Boolean = edge.history.exists(k => k._1 > min &&  k._1 <= max)
}
