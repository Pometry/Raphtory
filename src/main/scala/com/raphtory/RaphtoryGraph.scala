package com.raphtory

import akka.actor.{ActorSystem, Props}
import com.raphtory.core.actors.analysismanager.AnalysisRestApi.message._
import com.raphtory.core.actors.analysismanager.{AnalysisManager, AnalysisRestApi}
import com.raphtory.core.actors.orchestration.componentconnector.{AnalysisManagerConnector, PartitionConnector, BuilderConnector, SpoutConnector}
import com.raphtory.core.actors.graphbuilder.GraphBuilder
import com.raphtory.core.actors.orchestration.raphtoryleader.{WatchDog, WatermarkManager}
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

  val system = ActorSystem("Raphtory")

  system.actorOf(Props(new WatermarkManager()),"WatermarkManager")
  system.actorOf(Props(new WatchDog()), "WatchDog")

  system.actorOf(Props(new SpoutConnector(spout)), "Spoutmanager")
  system.actorOf(Props(new BuilderConnector(graphBuilder)), s"Builders")
  system.actorOf(Props(new PartitionConnector()), s"PartitionManager")

  val analysisManager = system.actorOf(Props(new AnalysisManagerConnector()), "AnalysisManagerConnector")

  //TODO tidy these, but will be done with full analysis Overhall
  def rangeQuery[S<:Object](analyser:Analyser[S],serialiser:AggregateSerialiser,start:Long,end:Long,increment:Long):Unit = {
    analysisManager ! RangeAnalysisRequest(analyser.getClass.getCanonicalName,serialiser.getClass.getCanonicalName,start,end,increment,List.empty,analyser.getArgs())
  }

  def rangeQuery[S<:Object](analyser:Analyser[S],serialiser:AggregateSerialiser,start:Long,end:Long,increment:Long,window:Long):Unit = {
    analysisManager ! RangeAnalysisRequest(analyser.getClass.getCanonicalName,serialiser.getClass.getCanonicalName,start,end,increment, List(window),analyser.getArgs())
  }
  def rangeQuery[S<:Object](analyser:Analyser[S],serialiser:AggregateSerialiser,start:Long,end:Long,increment:Long,windowBatch:List[Long]):Unit = {
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

  def liveQuery[S<:Object](analyser:Analyser[S],serialiser:AggregateSerialiser,repeat:Long,eventTime:Boolean):Unit = {
    analysisManager ! LiveAnalysisRequest(analyser.getClass.getCanonicalName,serialiser.getClass.getCanonicalName,repeat,eventTime, List.empty,analyser.getArgs())
  }

  def liveQuery[S<:Object](analyser:Analyser[S],serialiser:AggregateSerialiser,repeat:Long,eventTime:Boolean,window:Long):Unit = {
    analysisManager ! LiveAnalysisRequest(analyser.getClass.getCanonicalName,serialiser.getClass.getCanonicalName,repeat,eventTime,List(window),analyser.getArgs())
  }

  def liveQuery[S<:Object](analyser:Analyser[S],serialiser:AggregateSerialiser,repeat:Long,eventTime:Boolean,windowBatch:List[Long]):Unit = {
    analysisManager ! LiveAnalysisRequest(analyser.getClass.getCanonicalName,serialiser.getClass.getCanonicalName,repeat,eventTime, windowBatch,analyser.getArgs())
  }

}
