package com.raphtory.caseclass.clustercase

/**
  * Created by Mirate on 13/06/2017.
  */
import akka.actor.Props
import com.raphtory.Actors.RaphtoryActors.DataSource.{UpdateGen, bitcoinSpout}
import com.raphtory.caseclass.DocSvr

case class UpdateNode(seedLoc: String) extends DocSvr {
  implicit val system = init(List(seedLoc))
  system.actorOf(Props(new UpdateGen()), "UpdateGen")
}
