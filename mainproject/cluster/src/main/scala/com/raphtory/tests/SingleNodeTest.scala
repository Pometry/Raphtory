package com.raphtory.tests



import akka.actor.{ActorSystem, Props}
import com.raphtory.core.actors.{RaphtoryReplicator, WatchDog}
//this class creates an actor system with all of the required components for a Raphtory cluster
object SingleNodeTest extends App {


  val partitionNumber = 1
  val minimumRouters = 1
  val routerClassName = "com.raphtory.core.actors.router.RaphtoryWindowingRouter"
  val LamClassName = "com.raphtory.core.actors.analysismanager.TestLAM"
  val UpdaterName = "com.raphtory.core.actors.datasource.UpdateGen"

  val system = ActorSystem("Single-Node-test")

  system.actorOf(Props(new WatchDog(partitionNumber,minimumRouters)), "WatchDog")
  system.actorOf(Props(RaphtoryReplicator("Router", routerClassName)), s"Routers")
  system.actorOf(Props(RaphtoryReplicator("Router", routerClassName)), s"Routers")
  system.actorOf(Props(RaphtoryReplicator("Partition Manager")), s"PartitionManager")
  system.actorOf(Props(Class.forName(UpdaterName)), "UpdateGen")
  //system.actorOf(Props(Class.forName(LamClassName)), s"LiveAnalysisManager_$LamClassName")

}


