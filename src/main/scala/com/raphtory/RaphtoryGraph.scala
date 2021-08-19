package com.raphtory

import akka.actor.{ActorSystem, Props}
import com.raphtory.RaphtoryServer.{partitionCount, routerCount}
import com.raphtory.core.actors.analysismanager.AnalysisRestApi.message._
import com.raphtory.core.actors.analysismanager.{AnalysisManager, AnalysisRestApi}
import com.raphtory.core.actors.orchestration.componentconnector.{AnalysisManagerConnector, PartitionConnector, RouterConnector, SpoutConnector}
import com.raphtory.core.actors.graphbuilder.GraphBuilder
import com.raphtory.core.actors.orchestration.clustermanager.{WatchDog, WatermarkManager}
import com.raphtory.core.actors.spout.{Spout, SpoutAgent}
import com.raphtory.core.analysis.api.{AggregateSerialiser, Analyser}

object RaphtoryGraph {
  def apply[T](spout: Spout[T], graphBuilder: GraphBuilder[T]) : RaphtoryGraph[T] =
    new RaphtoryGraph(spout, graphBuilder)

  def apply[T](spoutPath: String, graphBuilderPath: String) : RaphtoryGraph[T] ={
    val spout = Class.forName(spoutPath).getConstructor().newInstance().asInstanceOf[Spout[T]]
    val graphBuilder = Class.forName(graphBuilderPath).getConstructor().newInstance().asInstanceOf[GraphBuilder[T]]
    new RaphtoryGraph(spout, graphBuilder)
  }

}

class RaphtoryGraph[T](spout: Spout[T], graphBuilder: GraphBuilder[T]) {
  //Kamon.init() //start tool logging

//  val root = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[ch.qos.logback.classic.Logger]
  //root.setLevel(Level.ERROR)
  val system = ActorSystem("Raphtory")

  val partitionNumber = 1
  val minimumRouters  = 1
  system.actorOf(Props(new WatermarkManager(partitionNumber)),"WatermarkManager")
  system.actorOf(Props(new WatchDog(partitionNumber, minimumRouters)), "WatchDog")

  system.actorOf(Props(new SpoutConnector(partitionCount,routerCount,spout)), "Spoutmanager")
  system.actorOf(Props(new RouterConnector( partitionNumber, minimumRouters,graphBuilder)), s"Routers")
  system.actorOf(Props(new PartitionConnector(partitionNumber,minimumRouters)), s"PartitionManager")

  val analysisManager = system.actorOf(Props(new AnalysisManagerConnector(partitionCount,routerCount)), "AnalysisManagerConnector")

  //TODO tidy these, but will be done with full analysis Overhall
  def rangeQuery[S<:Serializable](analyser:Analyser[S],serialiser:AggregateSerialiser,start:Long,end:Long,increment:Long):Unit = {
    analysisManager ! RangeAnalysisRequest(analyser.getClass.getCanonicalName,serialiser.getClass.getCanonicalName,start,end,increment,List.empty,analyser.getArgs(),"")
  }

  def rangeQuery[S<:Serializable](analyser:Analyser[S],serialiser:AggregateSerialiser,start:Long,end:Long,increment:Long,window:Long):Unit = {
    analysisManager ! RangeAnalysisRequest(analyser.getClass.getCanonicalName,serialiser.getClass.getCanonicalName,start,end,increment, List(window),analyser.getArgs(),"")
  }
  def rangeQuery[S<:Serializable](analyser:Analyser[S],serialiser:AggregateSerialiser,start:Long,end:Long,increment:Long,windowBatch:List[Long]):Unit = {
    analysisManager ! RangeAnalysisRequest(analyser.getClass.getCanonicalName,serialiser.getClass.getCanonicalName,start,end,increment,windowBatch,analyser.getArgs(),"")
  }

  def viewQuery[S<:Serializable](analyser:Analyser[S],serialiser:AggregateSerialiser,timestamp:Long):Unit = {
    analysisManager ! ViewAnalysisRequest(analyser.getClass.getCanonicalName,serialiser.getClass.getCanonicalName,timestamp,List.empty,analyser.getArgs(),"")
  }

  def viewQuery[S<:Serializable](analyser:Analyser[S],serialiser:AggregateSerialiser,timestamp:Long,window:Long):Unit = {
    analysisManager ! ViewAnalysisRequest(analyser.getClass.getCanonicalName,serialiser.getClass.getCanonicalName,timestamp, List(window),analyser.getArgs(),"")
  }
  def viewQuery[S<:Serializable](analyser:Analyser[S],serialiser:AggregateSerialiser,timestamp:Long,windowBatch:List[Long]):Unit = {
    analysisManager ! ViewAnalysisRequest(analyser.getClass.getCanonicalName,serialiser.getClass.getCanonicalName,timestamp, windowBatch,analyser.getArgs(),"")
  }

  def liveQuery[S<:Serializable](analyser:Analyser[S],serialiser:AggregateSerialiser,repeat:Long,eventTime:Boolean):Unit = {
    analysisManager ! LiveAnalysisRequest(analyser.getClass.getCanonicalName,serialiser.getClass.getCanonicalName,repeat,eventTime, List.empty,analyser.getArgs(),"")
  }

  def liveQuery[S<:Serializable](analyser:Analyser[S],serialiser:AggregateSerialiser,repeat:Long,eventTime:Boolean,window:Long):Unit = {
    analysisManager ! LiveAnalysisRequest(analyser.getClass.getCanonicalName,serialiser.getClass.getCanonicalName,repeat,eventTime,List(window),analyser.getArgs(),"")
  }

  def liveQuery[S<:Serializable](analyser:Analyser[S],serialiser:AggregateSerialiser,repeat:Long,eventTime:Boolean,windowBatch:List[Long]):Unit = {
    analysisManager ! LiveAnalysisRequest(analyser.getClass.getCanonicalName,serialiser.getClass.getCanonicalName,repeat,eventTime, windowBatch,analyser.getArgs(),"")
  }

}
