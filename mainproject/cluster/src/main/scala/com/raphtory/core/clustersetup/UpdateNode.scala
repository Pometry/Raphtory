package com.raphtory.core.clustersetup

/**
  * Created by Mirate on 13/06/2017.
  */
import akka.actor.ActorSystem
import akka.actor.Props

import scala.language.postfixOps

case class UpdateNode(seedLoc: String, className: String) extends DocSvr {
  implicit val system: ActorSystem = initialiseActorSystem(seeds = List(seedLoc))

  val actorName: String = "UpdateGen"
  system.actorOf(Props(Class.forName(className)), actorName)
}
