package com.raphtory

import akka.actor.ActorRef
import com.raphtory.core.components.akkamanagement.ComponentFactory
import com.raphtory.core.components.akkamanagement.RaphtoryActor._
import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.components.querymanager.QueryManager.Message.{LiveQuery, PointQuery, RangeQuery}
import com.raphtory.core.components.spout.Spout
import com.raphtory.core.model.algorithm.{GraphAlgorithm, GenericGraphPerspective}
import com.rits.cloning.Cloner

object RaphtoryPD {
  def apply[T](spout: Spout[T], graphBuilder: GraphBuilder[T]) : RaphtoryPD[T] =
    new RaphtoryPD(spout, graphBuilder)
}

class RaphtoryPD[T](spout: Spout[T], graphBuilder: GraphBuilder[T]) {
  var port = 1600
  val seedloc = "127.0.0.1:1600"

  val (watermarker,watchDog) = ComponentFactory.leader("127.0.0.1",port)
  val queryManager = ComponentFactory.query(seedloc,nextPort)

  val spoutRef = ComponentFactory.spout(seedloc,nextPort,spout)

  val builders =
    for(i<-0 until builderServers)
      yield ComponentFactory.builder(seedloc,nextPort,graphBuilder.asInstanceOf[GraphBuilder[Any]])//TODO tidy as instance of

  val partitions =
    for(i<-0 until partitionServers)
      yield ComponentFactory.partition(seedloc,nextPort)

  def getWatermarker()    :ActorRef = watermarker
  def getWatchdog()       :ActorRef = watchDog
  def getQueryManager()   :ActorRef = queryManager
  def getSpout()          :ActorRef = spoutRef
  def getBuilders()       :List[ActorRef] = builders.toList
  def getPartitions()     :List[ActorRef] = partitions.toList

  def pointQuery(graphAlgorithm: GraphAlgorithm,timestamp:Long,windows:List[Long]=List()) = {
    queryManager ! PointQuery(getID(graphAlgorithm),graphAlgorithm,timestamp,windows)
  }
  def rangeQuery(graphAlgorithm: GraphAlgorithm,start:Long, end:Long, increment:Long,windows:List[Long]=List()) = {
    queryManager ! RangeQuery(getID(graphAlgorithm),graphAlgorithm,start,end,increment,windows)
  }
  def liveQuery(graphAlgorithm: GraphAlgorithm,increment:Long,windows:List[Long]=List()) = {
    queryManager ! LiveQuery(getID(graphAlgorithm),graphAlgorithm,increment,windows)
  }

  private def nextPort() = {
    port = port+1
    port
  }

  private def getID(algorithm:GraphAlgorithm):String = {
    try{
      val path= algorithm.getClass.getCanonicalName.split("\\.")
      path(path.size-1)+"_" + System.currentTimeMillis()
    }
    catch {
      case e:NullPointerException => "Anon_Func_"+System.currentTimeMillis()
    }

  }

}
