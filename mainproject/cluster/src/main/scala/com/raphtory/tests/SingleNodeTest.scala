package com.raphtory.tests

import akka.actor.{Actor, ActorSystem, Props}
import ch.qos.logback.classic.Level
import com.raphtory.core.analysis.Managers.RangeManagers.WindowedRangeAnalysisManager
import com.raphtory.core.analysis.API.{Analyser, WorkerID}
import com.raphtory.core.components.ClusterManagement.{RaphtoryReplicator, WatchDog}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.language.postfixOps

object SingleNodeTest extends App {
  val root = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[ch.qos.logback.classic.Logger]
  root.setLevel(Level.ERROR)

  val partitionNumber = 1
  val minimumRouters = 1


 var UpdaterName = "com.raphtory.examples.gab.actors.GabExampleSpout"
 var routerClassName = "com.raphtory.examples.gab.actors.GabPostGraphRouter"
 val Analyser = "com.raphtory.core.analysis.Algorithms.Density"

  val system = ActorSystem("Single-Node-test")

  system.actorOf(Props(new WatchDog(partitionNumber,minimumRouters)), "WatchDog")
  system.actorOf(Props(RaphtoryReplicator("Router",1, routerClassName)), s"Routers")
  system.actorOf(Props(RaphtoryReplicator("Partition Manager",1)), s"PartitionManager")
  system.actorOf(Props(Class.forName(UpdaterName)), "UpdateGen")


  val analyser = Class.forName(Analyser).newInstance().asInstanceOf[Analyser]

//  val end = 1525368897000L
//window//
  val start = 1470783600000L
  val end = 1471891600000L
  val jump =    3600000
  val window =    3600000
  system.actorOf(Props(new WindowedRangeAnalysisManager("testname",analyser,start,end,jump,window)), s"LiveAnalysisManager_$Analyser")

////////////////


//////////  //range////
//  val start = 1470783600000L
//  //val end =   1471459626000L
//  val end=1525368897000L
//  val jump = 3600000*24


////  //range////
//  val start = 1470783600000L
//  //val end =   1471459626000L
//  //
//  val end=1471891600000L
//  //val end=1525368897000L
//  val jump = 3600000*24
//
//  val cl = Class.forName(LamClassName)
//  val cons = cl.getConstructor(classOf[String],classOf[Long],classOf[Long],classOf[Long])
//  system.actorOf(Props(cons.newInstance("testName",start.asInstanceOf[AnyRef],end.asInstanceOf[AnyRef],jump.asInstanceOf[AnyRef]).asInstanceOf[Actor]), s"LiveAnalysisManager_$LamClassName")
//val cl = Class.forName(LamClassName)
//  val cons = cl.getConstructor(classOf[String],classOf[Long],classOf[Long],classOf[Long],classOf[Long])
//  system.actorOf(Props(cons.newInstance("testName",start.asInstanceOf[AnyRef],end.asInstanceOf[AnyRef],jump.asInstanceOf[AnyRef],window.asInstanceOf[AnyRef]).asInstanceOf[Actor]), s"LiveAnalysisManager_$LamClassName")
}

