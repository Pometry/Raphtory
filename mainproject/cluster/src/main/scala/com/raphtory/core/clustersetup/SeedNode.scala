package com.raphtory.core.clustersetup

import akka.actor.{ActorSystem, Props}
import com.raphtory.core.components.ClusterManagement.SeedActor
import com.typesafe.config.{Config, ConfigFactory}

case class SeedNode(seedLoc: String) extends DocSvr {
  val conf: Config = ConfigFactory.load()
  implicit val system: ActorSystem = initialiseActorSystem(seeds = List(seedLoc))

  system.log.info("Bleep Bloop I am a seed node.")

  val actorName: String = "cluster"
  system.actorOf(Props(new SeedActor(svr = this)), actorName)
}
