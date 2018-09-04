package com.raphtory.core.clustersetup.singlenode

import akka.actor.Props
import com.raphtory.core.actors.{RaphtoryReplicator, SeedActor, WatchDog}
import com.raphtory.core.clustersetup.DocSvr
import com.typesafe.config.{Config, ConfigFactory}
import scala.language.postfixOps
import scala.sys.process._

case class SingleNodeSetup(seedLoc:String,routerClassName:String,UpdaterName:String,LAMName:String,partitionNumber:Int,minimumRouters:Int) extends DocSvr {
  val conf : Config = ConfigFactory.load()
  implicit val system = init(List(seedLoc))
  "redis-server --daemonize yes" ! //start redis running on manager partition

  system.actorOf(Props(new SeedActor(this)), "cluster")
  system.actorOf(Props(new WatchDog(partitionNumber,minimumRouters)), "WatchDog")
  system.actorOf(Props(RaphtoryReplicator("Router",1, routerClassName)), s"Routers")
  system.actorOf(Props(RaphtoryReplicator("Partition Manager",1)), s"PartitionManager")
  system.actorOf(Props(Class.forName(UpdaterName)), "UpdateGen")
  system.actorOf(Props(Class.forName(LAMName)), s"LiveAnalysisManager_$LAMName")

}