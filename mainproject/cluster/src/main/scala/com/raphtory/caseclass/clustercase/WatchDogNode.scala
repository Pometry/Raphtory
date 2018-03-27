package com.raphtory.caseclass.clustercase

/**
  * Created by Mirate on 11/07/2017.
  */
import akka.actor.Props
import com.raphtory.Actors.RaphtoryActors.WatchDogActor
import com.raphtory.caseclass.DocSvr

case class WatchDogNode(seedLoc: String, partitionNumber: String)
    extends DocSvr {
  implicit val system = init(List(seedLoc))
  system.actorOf(Props(new WatchDogActor(partitionNumber.toInt)), "WatchDog")
}
