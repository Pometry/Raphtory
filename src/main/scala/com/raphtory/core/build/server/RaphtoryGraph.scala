package com.raphtory.core.build.server

import akka.actor.{ActorRef, ActorSystem, Props}
import com.raphtory.core.build.server.RaphtoryService.spout
import com.raphtory.core.components.analysismanager.AnalysisRestApi.message.{LiveAnalysisRequest, RangeAnalysisRequest, ViewAnalysisRequest}
import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.components.leader.{WatchDog, WatermarkManager}
import com.raphtory.core.components.management.ComponentFactory
import com.raphtory.core.components.management.RaphtoryActor.{builderServers, partitionServers}
import com.raphtory.core.components.management.connectors.{BuilderConnector, PartitionConnector, QueryManagerConnector, SpoutConnector}
import com.raphtory.core.components.querymanager.QueryManager.Message.{LiveQuery, PointQuery, RangeQuery}
import com.raphtory.core.components.spout.Spout
import com.raphtory.core.model.algorithm.{AggregateSerialiser, Analyser, GraphAlgorithm}

class RaphtoryGraph [T](spout: Spout[T], graphBuilder: GraphBuilder[T]) {
    val system = ComponentFactory.initialiseActorSystem(List("127.0.0.1:1600"),1600)
      private val watchDog = system.actorOf(Props(new WatchDog()), "WatchDog")
      system.actorOf(Props(new WatermarkManager(watchDog)),"WatermarkManager")


      system.actorOf(Props(new SpoutConnector(spout)), "Spoutmanager")

      for(i<-0 until builderServers)
        system.actorOf(Props(new BuilderConnector(graphBuilder)), s"Builder_$i")

      for(i<-0 until partitionServers)
        system.actorOf(Props(new PartitionConnector()), s"PartitionManager_$i")

    //  val analysisManager = system.actorOf(Props(new AnalysisManagerConnector()), "AnalysisManagerConnector")
    val queryManager    = system.actorOf(Props(new QueryManagerConnector()), "QueryManagerConnector")


    def pointQuery(graphAlgorithm: GraphAlgorithm,timestamp:Long,windows:List[Long]=List()) = {
      queryManager ! PointQuery(graphAlgorithm,timestamp,windows)
    }
    def rangeQuery(graphAlgorithm: GraphAlgorithm,start:Long, end:Long, increment:Long,windows:List[Long]=List()) = {
      queryManager ! RangeQuery(graphAlgorithm,start,end,increment,windows)
    }
    def liveQuery(graphAlgorithm: GraphAlgorithm,increment:Long,windows:List[Long]=List()) = {
      queryManager ! LiveQuery(graphAlgorithm,increment,windows)
    }

  }
