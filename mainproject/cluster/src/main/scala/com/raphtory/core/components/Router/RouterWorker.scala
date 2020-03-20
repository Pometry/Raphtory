package com.raphtory.core.components.Router

import akka.actor.Actor
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.model.communication.{AllocateJob, GraphUpdate, UpdatedCounter}
import com.raphtory.core.utils.Utils.getManager

trait RouterWorker extends Actor {
  protected final val mediator = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)

  protected def       initialManagerCount : Int
  protected def parseTuple(value: Any)

  private   var       managerCount : Int = initialManagerCount
  protected final def getManagerCount = managerCount
  private var count = 0

  override def receive = {
    case UpdatedCounter(newValue) => newPmJoined(newValue)
    case AllocateJob(record) => {parseTuple(record)}
  }

  def sendGraphUpdate[T <: GraphUpdate](message:T): Unit = mediator ! DistributedPubSubMediator.Send(getManager(message.srcID, getManagerCount), message , false)
  private def newPmJoined(newValue : Int) = if (managerCount < newValue) managerCount = newValue


}
