package com.raphtory.core.actors.router.TraditionalRouter.Helpers

import akka.actor.Actor
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.model.communication.{AllocateJob, RaphWriteClass, UpdatedCounter}
import com.raphtory.core.utils.Utils.getManager

trait RouterSlave extends Actor {
  protected final val mediator = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)

  protected def       initialManagerCount : Int
  protected def parseRecord(value: Any)

  private   var       managerCount : Int = initialManagerCount
  protected final def getManagerCount = managerCount
  private var count = 0

  override def receive = {
    case UpdatedCounter(newValue) => newPmJoined(newValue)
    case AllocateJob(record) => parseRecord(record)
  }

  def toPartitionManager[T <: RaphWriteClass](message:T): Unit = mediator ! DistributedPubSubMediator.Send(getManager(message.srcId, getManagerCount), message , false)
  private def newPmJoined(newValue : Int) = if (managerCount < newValue) managerCount = newValue


}
