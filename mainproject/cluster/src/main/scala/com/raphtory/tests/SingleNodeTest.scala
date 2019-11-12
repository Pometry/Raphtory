package com.raphtory.tests

import akka.actor.{Actor, ActorSystem, Props}
import ch.qos.logback.classic.Level
import com.raphtory.core.analysis.Managers.RangeManagers.{BWindowedRangeAnalysisManager, RangeAnalysisManager, WindowedRangeAnalysisManager}
import com.raphtory.core.analysis.API.{Analyser, WorkerID}
import com.raphtory.core.analysis.Managers.LiveManagers.LiveAnalysisManager
import com.raphtory.core.analysis.Managers.ViewManagers.BWindowedViewAnalysisManager
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
 var routerClassName = "com.raphtory.examples.gab.actors.GabUserGraphRouter"
 val Analyser = "com.raphtory.core.analysis.Algorithms.ConnectedComponents"
 //var UpdaterName = "com.raphtory.examples.ethereum.actors.EthereumPostgresSpout"
 //var routerClassName = "com.raphtory.examples.ethereum.actors.EthereumTransactionRouter"
 //val Analyser = "com.raphtory.examples.ethereum.analysis.DegreeRanking"
 val system = ActorSystem("Single-Node-test")

  system.actorOf(Props(new WatchDog(partitionNumber,minimumRouters)), "WatchDog")
  system.actorOf(Props(RaphtoryReplicator("Router",1, routerClassName)), s"Routers")
  system.actorOf(Props(RaphtoryReplicator("Partition Manager",1)), s"PartitionManager")
  system.actorOf(Props(Class.forName(UpdaterName)), "UpdateGen")

  val analyser = Class.forName(Analyser).newInstance().asInstanceOf[Analyser]

//  val end = 1525368897000L
  //1470783600000L 1471388400000L
//window//
  Thread.sleep(30000)
  println("starting")
  //val start = 1476113850000L
 val start = 1474326000000L
  //val end = 1476113855000L
 val end = 1525368897000L
  val jump =    86400000
  val window =    86400000

//  val start = 1439311261000L
//  val end =   31525368897000L
//  val jump =    3600000
//  val window =    3600000
//
  val windowset:Array[Long] = Array(31536000000L,2592000000L,604800000,86400000,3600000)
  system.actorOf(Props(new BWindowedViewAnalysisManager("testname",analyser,start,windowset)), s"LiveAnalysisManager_$Analyser")

////////////////
// {"time":1474326000000,"windowsize":31536000000,"biggest":3984,"total":63,"totalWithoutIslands":24,"totalIslands":39,"proportion":0.97911036,"proportionWithoutIslands":0.9885856,"clustersGT2":1,"viewTime":1403,"concatTime":1},
// {"time":1474326000000,"windowsize":2592000000,"biggest":3943,"total":62,"totalWithoutIslands":24,"totalIslands":38,"proportion":0.9791408,"proportionWithoutIslands":0.9884683,"clustersGT2":1,"viewTime":1403,"concatTime":1},
// {"time":1474326000000,"windowsize":604800000,"biggest":1956,"total":46,"totalWithoutIslands":26,"totalIslands":20,"proportion":0.96497285,"proportionWithoutIslands":0.97458893,"clustersGT2":2,"viewTime":1403,"concatTime":0},
// {"time":1474326000000,"windowsize":86400000,"biggest":659,"total":31,"totalWithoutIslands":24,"totalIslands":7,"proportion":0.92296916,"proportionWithoutIslands":0.9321075,"clustersGT2":3,"viewTime":1403,"concatTime":1},


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

