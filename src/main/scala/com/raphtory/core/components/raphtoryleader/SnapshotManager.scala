package com.raphtory.core.components.raphtoryleader

import akka.actor.ActorRef
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import WatermarkManager.Message.{SaveState, WatermarkTime}
import com.raphtory.core.components.actor.RaphtoryActor

class SnapshotManager(managerCount: Int) extends RaphtoryActor {

  override def receive: Receive = {
    case WatermarkTime(time:Long) => saveState(time)
  }

  def saveState(time:Long) =
    getAllWriters().foreach { workerPath =>mediator ! new DistributedPubSubMediator.Send(workerPath, SaveState)}
}


//