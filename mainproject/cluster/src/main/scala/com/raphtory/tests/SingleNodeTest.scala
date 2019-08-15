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

 var UpdaterName = "com.raphtory.examples.gabMining.actors.GabMiningSpout"
 var routerClassName = "com.raphtory.examples.gabMining.actors.GabMiningRouter"
// var LamClassName = "com.raphtory.examples.bitcoin.actors.BitcoinLiveAnalysisManager"
 val LamClassName = "com.raphtory.examples.GenericAlgorithms.Density.DensityRAM"

  //val LamClassName = "com.raphtory.examples.GenericAlgorithms.Density.GabMiningDensityRange"
 // val LamClassName = "com.raphtory.examples.GenericAlgorithms.Density.GabMiningDensityWindow"
  //val LamClassName = "com.raphtory.examples.GenericAlgorithms.Density.GabMiningDensityLAM"

  //val LamClassName = "com.raphtory.examples.gabMining.actors.GabMiningDistribRangeIn"
  //val LamClassName = "com.raphtory.examples.gabMining.actors.GabMiningDistribRangeOut"

  //val LamClassName = "com.raphtory.examples.gabMining.actors.GabMiningStarsWindow"
   // val LamClassName = "com.raphtory.examples.gabMining.actors.GabMiningStarsRange"

   // val LamClassName = "com.raphtory.examples.gabMining.actors.GabMiningCCWindow"\
  //LamClassName = "com.raphtory.examples.gabMining.actors.GabMiningCCRange"

  //var LamClassName = "com.raphtory.examples.GenericAlgorithms.ConnectedComponents.ConComLAM"
  //var LamClassName="com.raphtory.examples.gabMining.actors.GabMiningDiameterLAM"

  //val LamClassName = "com.raphtory.examples.gabMining.actors.GabMiningStarsLAM"
  //val LamClassName = "com.raphtory.examples.gabMining.actors.GabMiningLAMOptim"
  //val LamClassName = "com.raphtory.examples.gabMining.actors.GabMiningDistribLAM"

//  val LamClassName = "com.raphtory.examples.gabMining.actors.GabMiningCCWindow"
 // val LamClassName = "com.raphtory.examples.gabMining.actors.GabMiningCCWindowHr"
//  val LamClassName = "com.raphtory.examples.gabMining.actors.GabMiningCCRange"
  // val LamClassName = "com.raphtory.examples.GenericAlgorithms.ConnectedComponents.ConComLAM"

  //val LamClassName = "com.raphtory.examples.gabMining.actors.GabMiningDistribLAM"
  //val LamClassName = "com.raphtory.examples.GenericAlgorithms.Density.GabMiningDensityLAM"

  val system = ActorSystem("Single-Node-test")

  system.actorOf(Props(new WatchDog(partitionNumber,minimumRouters)), "WatchDog")
  system.actorOf(Props(RaphtoryReplicator("Router",1, routerClassName)), s"Routers")
  system.actorOf(Props(RaphtoryReplicator("Partition Manager",1)), s"PartitionManager")
  // system.actorOf(Props(RaphtoryReplicator("Partition Manager",2)), s"PartitionManager2")
  system.actorOf(Props(Class.forName(UpdaterName)), "UpdateGen")


//LAM
//  val cl = Class.forName(LamClassName)
//  val cons = cl.getConstructor(classOf[String])
//
//  system.actorOf(Props( cons.newInstance("testName").asInstanceOf[Actor]), s"LiveAnalysisManager_$LamClassName")

//window//
//val start = 1470783600000L
//  val end = 1525368897000L
//  val jump =    3600000
//  val window =    3600000
//
//
//  val cl = Class.forName(LamClassName)
//  val cons = cl.getConstructor(classOf[String],classOf[Long],classOf[Long],classOf[Long],classOf[Long])
//  system.actorOf(Props(cons.newInstance("testName",start.asInstanceOf[AnyRef],end.asInstanceOf[AnyRef],jump.asInstanceOf[AnyRef],window.asInstanceOf[AnyRef]).asInstanceOf[Actor]), s"LiveAnalysisManager_$LamClassName")
////////////////
//View//
//  val start = 1471459626000L
//  val cl = Class.forName(LamClassName)
//  val cons = cl.getConstructor(classOf[String],classOf[Long])
//  system.actorOf(Props(cons.newInstance("testName",start.asInstanceOf[AnyRef]).asInstanceOf[Actor]), s"LiveAnalysisManager_$LamClassName")
////////////////
////
////  //range////
  val start = 1470783600000L
  //val end =   1471459626000L
  val end=1525368897000L
  val jump = 3600000*24

  val cl = Class.forName(LamClassName)
  val cons = cl.getConstructor(classOf[String],classOf[Long],classOf[Long],classOf[Long])
  system.actorOf(Props(cons.newInstance("testName",start.asInstanceOf[AnyRef],end.asInstanceOf[AnyRef],jump.asInstanceOf[AnyRef]).asInstanceOf[Actor]), s"LiveAnalysisManager_$LamClassName")


}

