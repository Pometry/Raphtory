package com.raphtory.tests

import akka.actor.{ActorSystem, Props}
import ch.qos.logback.classic.Level
import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.analysis.Managers.RangeManagers.{BWindowedRangeAnalysisManager, RangeAnalysisManager}
import com.raphtory.core.components.ClusterManagement.{RaphtoryReplicator, WatchDog}
import org.slf4j.LoggerFactory

import scala.language.postfixOps

object SingleNodeTest extends App {
  val root = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[ch.qos.logback.classic.Logger]
  root.setLevel(Level.ERROR)

  val partitionNumber = 1
  val minimumRouters = 1



 var Analyser = "com.raphtory.core.analysis.Algorithms.ConnectedComponents"
 Analyser = "com.raphtory.core.analysis.Algorithms.DegreeRanking"

 var UpdaterName = "com.raphtory.examples.ldbc.spouts.LDBCSpout"
 var routerClassName = "com.raphtory.examples.ldbc.routers.LDBCRouter"
 val start = 1262307661000L
 val end =   1357002061000L
 val jump =    86400000

 //ether test
 //val start = 5000000L
 //val end = 6000000L
 //val jump =    1
 //var UpdaterName = "com.raphtory.examples.blockchain.spouts.EthereumGethSpout"
 //var routerClassName = "com.raphtory.examples.blockchain.routers.EthereumGethRouter"
 //val Analyser = "com.raphtory.examples.blockchain.analysers.EthereumDegreeRanking"

 //Gab test
 //  val start = 1470837600000L
 //  val end =   31525368897000L
 //  val jump =    3600000
 //var UpdaterName = "com.raphtory.examples.gab.actors.GabExampleSpout"
 //var routerClassName = "com.raphtory.examples.gab.actors.GabUserGraphRouter"





 val system = ActorSystem("Single-Node-test")

  system.actorOf(Props(new WatchDog(partitionNumber,minimumRouters)), "WatchDog")
  system.actorOf(Props(RaphtoryReplicator("Router",1, routerClassName)), s"Routers")
  system.actorOf(Props(RaphtoryReplicator("Partition Manager",1)), s"PartitionManager")
  system.actorOf(Props(Class.forName(UpdaterName)), "UpdateGen")

  val analyser = Class.forName(Analyser).newInstance().asInstanceOf[Analyser]

  Thread.sleep(360000)
  println("Starting Analysis")








  //val windowset:Array[Long] = Array(31536000000L,2592000000L,604800000,86400000,3600000)

  system.actorOf(Props(new RangeAnalysisManager("testname",analyser,start,end,jump)), s"LiveAnalysisManager_$Analyser")

////////////////
//  {"time":1474326000000,"windowsize":31536000000,"biggest":3990,"total":64,"totalWithoutIslands":24,"totalIslands":40,"proportion":0.9789009,"proportionWithoutIslands":0.9886026,"clustersGT2":1,"viewTime":2391,"concatTime":7},
//  {"time":1474326000000,"windowsize":2592000000,"biggest":3948,"total":63,"totalWithoutIslands":24,"totalIslands":39,"proportion":0.97892386,"proportionWithoutIslands":0.9884827,"clustersGT2":1,"viewTime":2391,"concatTime":2},
//  {"time":1474326000000,"windowsize":604800000,"biggest":1945,"total":48,"totalWithoutIslands":27,"totalIslands":21,"proportion":0.9633482,"proportionWithoutIslands":0.9734735,"clustersGT2":2,"viewTime":2391,"concatTime":1},
//  {"time":1474326000000,"windowsize":86400000,"biggest":658,"total":36,"totalWithoutIslands":27,"totalIslands":9,"proportion":0.9126214,"proportionWithoutIslands":0.9241573,"clustersGT2":3,"viewTime":2391,"concatTime":1},
//  {"time":1474326000000,"windowsize":3600000,"biggest":56,"total":22,"totalWithoutIslands":20,"totalIslands":2,"proportion":0.49557522,"proportionWithoutIslands":0.5045045,"clustersGT2":8,"viewTime":2391,"concatTime":0},


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

