package com.raphtory.core.components.orchestration.raphtoryleader

import akka.actor.ActorRef
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.components.RaphtoryActor
import com.raphtory.core.components.orchestration.raphtoryleader.WatermarkManager.Message.{SaveState, WatermarkTime}

class SnapshotManager(managerCount: Int) extends RaphtoryActor {

  override def receive: Receive = {
    case WatermarkTime(time:Long) => saveState(time)
  }

  def saveState(time:Long) =
    getAllWriters().foreach { workerPath =>mediator ! new DistributedPubSubMediator.Send(workerPath, SaveState)}
}


//