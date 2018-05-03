package com.raphtory.core.clustersetup

/**
  * Created by Mirate on 11/07/2017.
  */
import akka.actor.Props
import com.raphtory.core.actors.WatchDog

case class WatchDogNode(seedLoc: String, partitionNumber: Int,minimumRouters:Int)
    extends DocSvr {
  implicit val system = init(List(seedLoc))
  system.actorOf(Props(new WatchDog(partitionNumber,minimumRouters)), "WatchDog")
}
