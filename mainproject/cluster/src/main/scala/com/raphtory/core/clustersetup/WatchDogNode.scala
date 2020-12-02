package com.raphtory.core.clustersetup

/**
  * Created by Mirate on 11/07/2017.
  */
import akka.actor.ActorSystem
import akka.actor.Props
import com.raphtory.core.components.ClusterManagement.WatchDog
import com.raphtory.core.components.ClusterManagement.WatermarkManager

case class WatchDogNode(seedLoc: String, partitionNumber: Int, minimumRouters: Int) extends DocSvr {
  implicit val system: ActorSystem = initialiseActorSystem(seeds = List(seedLoc))
  system.actorOf(Props(new WatermarkManager(managerCount = 1)), "WatermarkManager")
  system.actorOf(
          Props(WatchDog(managerCount = partitionNumber, minimumRouters = minimumRouters))
            .withDispatcher("misc-dispatcher"),
          "WatchDog"
  )
}
