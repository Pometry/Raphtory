package com.raphtory.core.build.client

import akka.actor.{ActorRef, Props}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.components.leader.WatchDog.Message.SpoutUp
import com.raphtory.core.components.akkamanagement.ComponentFactory
import com.raphtory.core.components.querymanager.QueryManager.Message.{LiveQuery, PointQuery, RangeQuery}
import com.raphtory.core.model.algorithm.{GraphAlgorithm, GenericGraphPerspective}

class RaphtoryClient(leader:String,port:Int) {
  val system = ComponentFactory.initialiseActorSystem(List(leader),port)
  val handler = system.actorOf(Props(new ClientHandler),"clientHandler")

  private def getID(algorithm:GraphAlgorithm):String = {
    try{
      val path= algorithm.getClass.getCanonicalName.split("\\.")
      path(path.size-1)+"_" + System.currentTimeMillis()
    }
    catch {
      case e:NullPointerException => "Anon_Func_"+System.currentTimeMillis()
    }

  }

  def pointQuery(algorithm: GraphAlgorithm, timestamp:Long, windows:List[Long]=List()) = {
    handler ! PointQuery(getID(algorithm),algorithm,timestamp,windows)
  }
  def rangeQuery(algorithm: GraphAlgorithm, start:Long, end:Long, increment:Long, windows:List[Long]=List()) = {
    handler ! RangeQuery(getID(algorithm),algorithm,start,end,increment,windows)
  }
  def liveQuery(algorithm: GraphAlgorithm, increment:Long, windows:List[Long]=List()) = {
    handler ! LiveQuery(getID(algorithm),algorithm,increment,windows)
  }

}
