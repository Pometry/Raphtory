package com.raphtory

import akka.actor.{ActorSystem, Props}
import com.raphtory.core.actors.AnalysisManager.AnalysisRestApi.message._
import com.raphtory.core.actors.AnalysisManager.{AnalysisManager, AnalysisRestApi}
import com.raphtory.core.actors.ClusterManagement.componentConnector.{PartitionConnector, RouterConnector}
import com.raphtory.core.actors.ClusterManagement.{WatchDog, WatermarkManager}
import com.raphtory.core.actors.Router.GraphBuilder
import com.raphtory.core.actors.Spout.{Spout, SpoutAgent}
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
  val system = ActorSystem("Citation-system")

  val partitionNumber = 1
  val minimumRouters  = 1
  system.actorOf(Props(new WatermarkManager(partitionNumber)),"WatermarkManager")
  system.actorOf(Props(new WatchDog(partitionNumber, minimumRouters)), "WatchDog")

  system.actorOf(Props(new SpoutAgent(spout)), "Spout")
  system.actorOf(Props(new RouterConnector( partitionNumber, minimumRouters,graphBuilder)), s"Routers")
  system.actorOf(Props(new PartitionConnector(partitionNumber,minimumRouters)), s"PartitionManager")

  val analysisManager = system.actorOf(Props[AnalysisManager].withDispatcher("misc-dispatcher"), s"AnalysisManager")
  AnalysisRestApi(system)

  //TODO tidy these, but will be done with full analysis Overhall
  def rangeQuery[S<:Serializable](analyser:Analyser[S],serialiser:AggregateSerialiser,start:Long,end:Long,increment:Long,args:Array[String]):Unit = {
    analysisManager ! RangeAnalysisRequest(analyser.getClass.getCanonicalName,serialiser.getClass.getCanonicalName,start,end,increment,List.empty,args,"")
  }

  def rangeQuery[S<:Serializable](analyser:Analyser[S],serialiser:AggregateSerialiser,start:Long,end:Long,increment:Long,window:Long,args:Array[String]):Unit = {
    analysisManager ! RangeAnalysisRequest(analyser.getClass.getCanonicalName,serialiser.getClass.getCanonicalName,start,end,increment, List(window),args,"")
  }
  def rangeQuery[S<:Serializable](analyser:Analyser[S],serialiser:AggregateSerialiser,start:Long,end:Long,increment:Long,windowBatch:List[Long],args:Array[String]):Unit = {
    analysisManager ! RangeAnalysisRequest(analyser.getClass.getCanonicalName,serialiser.getClass.getCanonicalName,start,end,increment,windowBatch,args,"")
  }

  def viewQuery[S<:Serializable](analyser:Analyser[S],serialiser:AggregateSerialiser,timestamp:Long,args:Array[String]):Unit = {
    analysisManager ! ViewAnalysisRequest(analyser.getClass.getCanonicalName,serialiser.getClass.getCanonicalName,timestamp,List.empty,args,"")
  }

  def viewQuery[S<:Serializable](analyser:Analyser[S],serialiser:AggregateSerialiser,timestamp:Long,window:Long,args:Array[String]):Unit = {
    analysisManager ! ViewAnalysisRequest(analyser.getClass.getCanonicalName,serialiser.getClass.getCanonicalName,timestamp, List(window),args,"")
  }
  def viewQuery[S<:Serializable](analyser:Analyser[S],serialiser:AggregateSerialiser,timestamp:Long,windowBatch:List[Long],args:Array[String]):Unit = {
    analysisManager ! ViewAnalysisRequest(analyser.getClass.getCanonicalName,serialiser.getClass.getCanonicalName,timestamp, windowBatch,args,"")
  }

  def liveQuery[S<:Serializable](analyser:Analyser[S],serialiser:AggregateSerialiser,repeat:Long,eventTime:Boolean,args:Array[String]):Unit = {
    analysisManager ! LiveAnalysisRequest(analyser.getClass.getCanonicalName,serialiser.getClass.getCanonicalName,repeat,eventTime, List.empty,args,"")
  }

  def liveQuery[S<:Serializable](analyser:Analyser[S],serialiser:AggregateSerialiser,repeat:Long,eventTime:Boolean,window:Long,args:Array[String]):Unit = {
    analysisManager ! LiveAnalysisRequest(analyser.getClass.getCanonicalName,serialiser.getClass.getCanonicalName,repeat,eventTime,List(window),args,"")
  }

  def liveQuery[S<:Serializable](analyser:Analyser[S],serialiser:AggregateSerialiser,repeat:Long,eventTime:Boolean,windowBatch:List[Long],args:Array[String]):Unit = {
    analysisManager ! LiveAnalysisRequest(analyser.getClass.getCanonicalName,serialiser.getClass.getCanonicalName,repeat,eventTime, windowBatch,args,"")
  }

}
