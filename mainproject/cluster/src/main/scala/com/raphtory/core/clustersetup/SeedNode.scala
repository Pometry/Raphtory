package com.raphtory.core.clustersetup

import akka.actor.Props
import com.raphtory.core.actors.SeedActor
import com.typesafe.config.{Config, ConfigFactory}

case class SeedNode(seedLoc:String) extends DocSvr {
  val conf : Config = ConfigFactory.load()
  implicit val system = init(List(seedLoc))
  system.actorOf(Props(new SeedActor(this)), "cluster")
}
