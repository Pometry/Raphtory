package com.raphtory.tests



import akka.actor.{Actor, ActorSystem, Props}
import ch.qos.logback.classic.Level
import com.raphtory.core.components.AnalysisManager.LiveAnalysisManager
import com.raphtory.core.components.ClusterManagement.{RaphtoryReplicator, WatchDog}
import com.raphtory.core.model.graphentities.Vertex
import com.raphtory.core.storage.{EntityStorage, RaphtoryDBWrite}
import monix.execution.atomic.AtomicInt
import org.slf4j.LoggerFactory

import scala.collection.parallel.mutable.ParTrieMap
import scala.language.postfixOps
import scala.sys.process._
//this class creates an actor system with all of the required components for a Raphtory cluster
object SingleNodeTest extends App {
  val root = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[ch.qos.logback.classic.Logger]
  root.setLevel(Level.ERROR)

  val partitionNumber = 1
  val minimumRouters = 1




 // var routerClassName = "com.raphtory.examples.TestPackage.TestRouter"
 // var UpdaterName = "com.raphtory.examples.TestPackage.TestSpout"
  // var LamClassName = "com.raphtory.examples.bitcoin.actors.BitcoinLiveAnalysisManager"


  var UpdaterName = "com.raphtory.examples.gabMining.actors.GabMiningSpout"
  var routerClassName = "com.raphtory.examples.gabMining.actors.GabMiningRouter"
//var LamClassName = "com.raphtory.examples.GenericAlgorithms.ConnectedComponents.ConComLAM"
  //var LamClassName="com.raphtory.examples.gabMining.actors.GabMiningDiameterLAM"
  val LamClassName = "com.raphtory.examples.gabMining.actors.GabMiningLAM"
  //val LamClassName = "com.raphtory.examples.gabMining.actors.GabMiningDistribLAM"

  //val LamClassName = "com.raphtory.examples.gabMining.actors.GabMiningDensityLAM"

 // val UpdaterName = "com.raphtory.examples.gab.actors.GabSpout"


  val system = ActorSystem("Single-Node-test")

  system.actorOf(Props(new WatchDog(partitionNumber,minimumRouters)), "WatchDog")
  system.actorOf(Props(RaphtoryReplicator("Router",1, routerClassName)), s"Routers")
  system.actorOf(Props(RaphtoryReplicator("Partition Manager",1)), s"PartitionManager")
 // system.actorOf(Props(RaphtoryReplicator("Partition Manager",2)), s"PartitionManager2")
  system.actorOf(Props(Class.forName(UpdaterName)), "UpdateGen")

  val cl = Class.forName(LamClassName)
  val cons = cl.getConstructor(classOf[String],classOf[Long],classOf[Long],classOf[Long])
  system.actorOf(Props(cons.newInstance("testName",1470801546000L.asInstanceOf[AnyRef],1471459626000L.asInstanceOf[AnyRef],86400000.asInstanceOf[AnyRef]).asInstanceOf[Actor]), s"LiveAnalysisManager_$LamClassName")

  //Thread.sleep(60000)
  //println("hello there")
  //GraphRepoProxy.something

  //System.exit(0)

}

// val LamClassName = "com.raphtory.examples.gabMining.actors.GabMiningLAM"


  //val LamClassName = "com.raphtory.examples.random.actors.RandomLAM"
  //val routerClassName = "com.raphtory.examples.gab.actors.RaphtoryGabRouter"
 // val LamClassName = "com.raphtory.examples.gab.actors.GabLiveAnalyserManagerMostUsedTopics"
 //val


//val LamClassName = "com.raphtory.examples.gabMining.actors.GabMiningVerticesLAM"


//  val UpdaterName = "com.raphtory.examples.bitcoin.actors.BitcoinExampleSpout"
//  val routerClassName = "com.raphtory.examples.bitcoin.actors.BitcoinRaphtoryRouter"
//  val LamClassName = "com.raphtory.examples.bitcoin.actors.BitcoinLiveAnalysisManager"

//routerClassName = "com.raphtory.examples.gab.actors.RaphtoryGabRouter"
//val LamClassName = "com.raphtory.examples.random.actors.TestLAM"


//UpdaterName = "com.raphtory.examples.gab.actors.GabSpout"
//val routerClassName = "com.raphtory.examples.gab.actors.RaphtoryGabRouter"
//val LamClassName = "com.raphtory.examples.gab.actors.GabLiveAnalyserManagerMostUsedTopics"
//val UpdaterName = "com.raphtory.examples.gab.actors.GabSpout"


//val routerClassName = "com.raphtory.examples.random.actors.RaphtoryWindowingRouter"
// val LamClassName = "com.raphtory.core.actors.analysismanager.TestLAM"
// val UpdaterName = "com.raphtory.examples.random.actors.RandomSpout"


//  val routerClassName = "com.raphtory.examples.bitcoin.actors.BitcoinRaphtoryRouter"
//
//  val UpdaterName = "com.raphtory.examples.bitcoin.actors.BitcoinExampleSpout"
//  val LamClassName = "com.raphtory.examples.bitcoin.actors.BitcoinLiveAnalysisManager"


//val LamClassName = "com.raphtory.examples.random.actors.RandomLAM"
//val routerClassName = "com.raphtory.examples.gab.actors.RaphtoryGabRouter"
// val LamClassName = "com.raphtory.examples.gab.actors.GabLiveAnalyserManagerMostUsedTopics"


// var LamClassName = "com.raphtory.examples.bitcoin.actors.BitcoinLiveAnalysisManager"
// LamClassName = "com.raphtory.examples.gab.actors.GabLiveAnalyserManager"

//var LamClassName = "com.raphtory.examples.bitcoin.actors.BitcoinLiveAnalysisManager"
//LamClassName = "com.raphtory.examples.gab.actors.GabLiveAnalyserManager"

