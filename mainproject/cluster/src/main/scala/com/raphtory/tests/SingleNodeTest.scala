package com.raphtory.tests



import akka.actor.{ActorSystem, Props}
import ch.qos.logback.classic.Level
import com.raphtory.core.actors.{RaphtoryReplicator, WatchDog}
import com.raphtory.core.storage.{EntityStorage, RaphtoryDB}
import org.slf4j.LoggerFactory

import scala.language.postfixOps
import scala.sys.process._
//this class creates an actor system with all of the required components for a Raphtory cluster
object SingleNodeTest extends App {
  val root = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[ch.qos.logback.classic.Logger]
  root.setLevel(Level.ERROR)
  RaphtoryDB.clearDB()
  val partitionNumber = 1
  val minimumRouters = 1

  val routerClassName = "com.raphtory.examples.random.actors.RandomRouter"
  //val LamClassName = "com.raphtory.examples.random.actors.TestLAM"
  val UpdaterName = "com.raphtory.examples.random.actors.RandomSpout"
  //val routerClassName = "com.raphtory.examples.gab.actors.RaphtoryGabRouter"
  //val LamClassName = "com.raphtory.examples.gab.actors.GabLiveAnalyserManagerMostUsedTopics"
  //val UpdaterName = "com.raphtory.examples.gab.actors.GabSpout"


  //val routerClassName = "com.raphtory.examples.random.actors.RaphtoryWindowingRouter"
 // val LamClassName = "com.raphtory.core.actors.analysismanager.TestLAM"
 // val UpdaterName = "com.raphtory.examples.random.actors.RandomSpout"

  //val routerClassName = "com.raphtory.examples.bitcoin.actors.BitcoinRouter"
  //val LamClassName = "com.raphtory.examples.random.actors.TestLAM"
  //val UpdaterName = "com.raphtory.examples.bitcoin.actors.BitcoinSpout"
  //val routerClassName = "com.raphtory.examples.gab.actors.RaphtoryGabRouter"
 // val LamClassName = "com.raphtory.examples.gab.actors.GabLiveAnalyserManagerMostUsedTopics"
 // val UpdaterName = "com.raphtory.examples.gab.actors.GabSpout"


  val system = ActorSystem("Single-Node-test")

  system.actorOf(Props(new WatchDog(partitionNumber,minimumRouters)), "WatchDog")
  system.actorOf(Props(RaphtoryReplicator("Router",1, routerClassName)), s"Routers")
  system.actorOf(Props(RaphtoryReplicator("Partition Manager",1)), s"PartitionManager")
  system.actorOf(Props(Class.forName(UpdaterName)), "UpdateGen")


  Thread.sleep(100000)
  println("trying")
  println(EntityStorage.createSnapshot(EntityStorage.oldestTime+9000).get(1))
  //system.actorOf(Props(Class.forName(LamClassName)), s"LiveAnalysisManager_$LamClassName")

}


