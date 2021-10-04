package com.raphtory.core.build

import akka.actor.{ActorRef, ActorSystem, Props}
import com.raphtory.core.components.analysismanager.AnalysisRestApi.message._
import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.build.componentconnector.{AnalysisManagerConnector, BuilderConnector, PartitionConnector, QueryManagerConnector, SpoutConnector}
import com.raphtory.core.components.actor.RaphtoryActor._
import com.raphtory.core.components.querymanager.QueryManager.Message.{LiveQuery, PointQuery, RangeQuery}
import com.raphtory.core.components.raphtoryleader.{WatchDog, WatermarkManager}
import com.raphtory.core.components.spout.Spout
import com.raphtory.core.model.algorithm.{AggregateSerialiser, Analyser, GraphAlgorithm}
import com.raphtory.dev.wordSemantic.graphbuilders.CoMatGB
import com.raphtory.dev.wordSemantic.spouts.CoMatSpout

object RaphtoryNode {
  def apply[T](spout: Spout[T], graphBuilder: GraphBuilder[T]) : RaphtoryNode[T] =
    new RaphtoryNode(spout, graphBuilder)

}

class RaphtoryNode[T](spout: Spout[T], graphBuilder: GraphBuilder[T]) {
  val system = ActorSystem("Raphtory")
  var port = 1601
  val seedloc = "127.0.0.1:1601"

  val (watermarker,watchDog) = RaphtoryComponentFactory.leader(port)
  val analysisManager = RaphtoryComponentFactory.analysis(seedloc,nextPort)
  val queryManager = RaphtoryComponentFactory.query(seedloc,nextPort)

  val spoutRef = RaphtoryComponentFactory.spout(seedloc,nextPort,spout)

  val builders =
    for(i<-0 until builderServers)
      yield RaphtoryComponentFactory.builder(seedloc,nextPort,graphBuilder.asInstanceOf[GraphBuilder[Any]])//TODO tidy as instance of

  val partitions =
    for(i<-0 until partitionServers)
      yield RaphtoryComponentFactory.partition(seedloc,nextPort)

//  system.actorOf(Props(new WatermarkManager()),"WatermarkManager")
//  system.actorOf(Props(new WatchDog()), "WatchDog")
//
//  system.actorOf(Props(new SpoutConnector(spout)), "Spoutmanager")
//
//  for(i<-0 until builderServers)
//    system.actorOf(Props(new BuilderConnector(graphBuilder)), s"Builder_$i")
//
//  for(i<-0 until partitionServers)
//    system.actorOf(Props(new PartitionConnector()), s"PartitionManager_$i")
//
//  val analysisManager = system.actorOf(Props(new AnalysisManagerConnector()), "AnalysisManagerConnector")
//  val queryManager    = system.actorOf(Props(new QueryManagerConnector()), "QueryManagerConnector")
  
  def getWatermarker()    :ActorRef = watermarker
  def getWatchdog()       :ActorRef = watchDog
  def getAnalysisManager():ActorRef = analysisManager
  def getQueryManager()   :ActorRef = queryManager
  def getSpout()          :ActorRef = spoutRef
  def getBuilders()       :List[ActorRef] = builders.toList
  def getPartitions()     :List[ActorRef] = partitions.toList

  def pointQuery(graphAlgorithm: GraphAlgorithm,timestamp:Long,windows:List[Long]=List()) = {
    queryManager ! PointQuery(graphAlgorithm,timestamp,windows)
  }
  def rangeQuery(graphAlgorithm: GraphAlgorithm,start:Long, end:Long, increment:Long,windows:List[Long]=List()) = {
    queryManager ! RangeQuery(graphAlgorithm,start,end,increment,windows)
  }
  def liveQuery(graphAlgorithm: GraphAlgorithm,increment:Long,windows:List[Long]=List()) = {
    queryManager ! LiveQuery(graphAlgorithm,increment,windows)
  }

  //TODO tidy these, but will be done with full analysis Overhall
  def oldrangeQuery[S<:Object](analyser:Analyser[S], serialiser:AggregateSerialiser, start:Long, end:Long, increment:Long):Unit = {
    analysisManager ! RangeAnalysisRequest(analyser.getClass.getCanonicalName,serialiser.getClass.getCanonicalName,start,end,increment,List.empty,analyser.getArgs())
  }

  def oldrangeQuery[S<:Object](analyser:Analyser[S], serialiser:AggregateSerialiser, start:Long, end:Long, increment:Long, window:Long):Unit = {
    analysisManager ! RangeAnalysisRequest(analyser.getClass.getCanonicalName,serialiser.getClass.getCanonicalName,start,end,increment, List(window),analyser.getArgs())
  }
  def oldrangeQuery[S<:Object](analyser:Analyser[S], serialiser:AggregateSerialiser, start:Long, end:Long, increment:Long, windowBatch:List[Long]):Unit = {
    analysisManager ! RangeAnalysisRequest(analyser.getClass.getCanonicalName,serialiser.getClass.getCanonicalName,start,end,increment,windowBatch,analyser.getArgs())
  }

  def viewQuery[S<:Object](analyser:Analyser[S],serialiser:AggregateSerialiser,timestamp:Long):Unit = {
    analysisManager ! ViewAnalysisRequest(analyser.getClass.getCanonicalName,serialiser.getClass.getCanonicalName,timestamp,List.empty,analyser.getArgs())
  }

  def viewQuery[S<:Object](analyser:Analyser[S],serialiser:AggregateSerialiser,timestamp:Long,window:Long):Unit = {
    analysisManager ! ViewAnalysisRequest(analyser.getClass.getCanonicalName,serialiser.getClass.getCanonicalName,timestamp, List(window),analyser.getArgs())
  }
  def viewQuery[S<:Object](analyser:Analyser[S],serialiser:AggregateSerialiser,timestamp:Long,windowBatch:List[Long]):Unit = {
    analysisManager ! ViewAnalysisRequest(analyser.getClass.getCanonicalName,serialiser.getClass.getCanonicalName,timestamp, windowBatch,analyser.getArgs())
  }

  def oldliveQuery[S<:Object](analyser:Analyser[S], serialiser:AggregateSerialiser, repeat:Long, eventTime:Boolean):Unit = {
    analysisManager ! LiveAnalysisRequest(analyser.getClass.getCanonicalName,serialiser.getClass.getCanonicalName,repeat,eventTime, List.empty,analyser.getArgs())
  }

  def oldliveQuery[S<:Object](analyser:Analyser[S], serialiser:AggregateSerialiser, repeat:Long, eventTime:Boolean, window:Long):Unit = {
    analysisManager ! LiveAnalysisRequest(analyser.getClass.getCanonicalName,serialiser.getClass.getCanonicalName,repeat,eventTime,List(window),analyser.getArgs())
  }

  def oldliveQuery[S<:Object](analyser:Analyser[S], serialiser:AggregateSerialiser, repeat:Long, eventTime:Boolean, windowBatch:List[Long]):Unit = {
    analysisManager ! LiveAnalysisRequest(analyser.getClass.getCanonicalName,serialiser.getClass.getCanonicalName,repeat,eventTime, windowBatch,analyser.getArgs())
  }

  private def nextPort() = {
    port = port+1
    port
  }

}
