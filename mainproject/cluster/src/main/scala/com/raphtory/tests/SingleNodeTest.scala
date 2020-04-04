package com.raphtory.tests

import akka.actor.ActorSystem
import akka.actor.Props
import ch.qos.logback.classic.Level
import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.analysis.{AnalysisManager, AnalysisRestApi}
import com.raphtory.core.components.ClusterManagement.RaphtoryReplicator
import com.raphtory.core.components.ClusterManagement.WatchDog
import com.raphtory.core.model.communication.{LiveAnalysisRequest, RangeAnalysisRequest, ViewAnalysisRequest}
import org.slf4j.LoggerFactory

import scala.language.postfixOps

object SingleNodeTest extends App {
  val root = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[ch.qos.logback.classic.Logger]
  root.setLevel(Level.ERROR)

  val partitionNumber = 1
  val minimumRouters  = 1

  var Analyser = "com.raphtory.core.analysis.Algorithms.ConnectedComponents"
  Analyser = "com.raphtory.core.analysis.Algorithms.DegreeBasic"

  //var UpdaterName     = "com.raphtory.examples.ldbc.spouts.LDBCSpout"
  //var routerClassName = "com.raphtory.examples.ldbc.routers.LDBCRouter"
  //val start           = 1262394061000L
  //val end             = 1357002061000L
  //val jump            = 86400000
  //curl -X POST -H "Content-Type: application/json" --data '{"jsonrpc":"2.0", "jobID":"ldbcdegreeTest","analyserName":"com.raphtory.core.analysis.Algorithms.DegreeBasic","start":1262394061000,"end":1357002061000,"jump":86400000}' 127.0.0.1:8081/RangeAnalysisRequest

  //ether test
//  val start = 4000000L
//  val end = 6000000L
//  val jump =    10
//  var UpdaterName = "com.raphtory.examples.blockchain.spouts.EthereumGethSpout"
//  var routerClassName = "com.raphtory.examples.blockchain.routers.EthereumGethRouter"
//  Analyser = "com.raphtory.examples.blockchain.analysers.EthereumTaintTracking"

  //Gab test
    val start = 1470837600000L
    val end =   31525368897000L
    val jump =    3600000
    var UpdaterName = "com.raphtory.examples.gab.actors.GabExampleSpout"
    var routerClassName = "com.raphtory.examples.gab.actors.GabUserGraphRouter"

  //track and trace test
    //var UpdaterName = "com.raphtory.examples.trackAndTrace.spouts.TrackAndTraceSpout"
    //var routerClassName = "com.raphtory.examples.trackAndTrace.routers.TrackAndTraceRouter"
  //  Analyser = "com.raphtory.examples.blockchain.analysers.EthereumTaintTracking"


  val system = ActorSystem("Single-Node-test")

  system.actorOf(Props(new WatchDog(partitionNumber, minimumRouters)), "WatchDog")
  system.actorOf(Props(RaphtoryReplicator("Router", 1, routerClassName)), s"Routers")
  system.actorOf(Props(RaphtoryReplicator("Partition Manager", 1)), s"PartitionManager")
  system.actorOf(Props(Class.forName(UpdaterName)), "Spout")
  val analysisManager = system.actorOf(Props[AnalysisManager], s"AnalysisManager")
  AnalysisRestApi(system)

  //analysisManager ! ViewAnalysisRequest("jobID","com.raphtory.examples.blockchain.analysers.EthereumTaintTracking",1234L)

  //curl -X POST -H "Content-Type: application/json" --data '{"jsonrpc":"2.0", "jobID":"connectedComponentsTest","analyserName":"com.raphtory.core.analysis.Algorithms.ConnectedComponents"}' 127.0.0.1:8080/LiveAnalysisRequest
  //curl -X POST -H "Content-Type: application/json" --data '{"jsonrpc":"2.0", "jobID":"connectedComponentsViewTest","analyserName":"com.raphtory.core.analysis.Algorithms.ConnectedComponents","timestamp":1476113856000}' 127.0.0.1:8080/ViewAnalysisRequest
  //curl -X POST -H "Content-Type: application/json" --data '{"jsonrpc":"2.0", "jobID":"connectedComponentsRangeWindowTest","analyserName":"com.raphtory.core.analysis.Algorithms.ConnectedComponents","start":1475113856000,"end":1475113856000,"jump":3600000,"windowType":"batched","windowSet":[3600000,86000000]}' 127.0.0.1:8080/RangeAnalysisRequest

}
