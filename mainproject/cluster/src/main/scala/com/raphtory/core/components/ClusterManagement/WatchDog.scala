package com.raphtory.core.components.ClusterManagement

/**
  * Created by Mirate on 11/07/2017.
  */
import akka.actor.Actor
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.model.communication._
import com.raphtory.core.utils.Utils

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class WatchDog(managerCount:Int,minimumRouters:Int) extends Actor{
  val mediator = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)
  var pmcounter = 0
  var routercounter = 0
  var clusterUp = false
  val debug = false
  val maxTime = 30000

  var PMKeepAlive = TrieMap[Int,Long]()
  var RouterKeepAlive = TrieMap[Int,Long]()

  var pmCounter = 0
  var roCounter = 0

  override def preStart() {
    context.system.scheduler.schedule(Duration(2, SECONDS),Duration(10, SECONDS),self,"tick")
    context.system.scheduler.schedule(Duration(3, MINUTES),Duration(1, MINUTES),self,"refreshManagerCount")
  }

  override def receive: Receive = {
    case ClusterStatusRequest => sender() ! ClusterStatusResponse(clusterUp)
    case "tick" => keepAliveHandler()
    case "refreshManagerCount" =>
      mediator ! DistributedPubSubMediator.Publish(Utils.partitionsTopic, PartitionsCount(pmCounter))

    case RequestPartitionCount => {
      if(debug)println("sending out Partition Manager Count")
      sender() ! PartitionsCountResponse(pmCounter)
      //mediator ! DistributedPubSubMediator.Publish(Utils.partitionsTopic, PartitionsCount(pmCounter))
    }

    case PartitionUp(id:Int) => mapHandler(id,PMKeepAlive, "Partition Manager")
    case RouterUp(id:Int) =>mapHandler(id,RouterKeepAlive, "Router")

    case RequestPartitionId => newPMReqest()

    case RequestRouterId => newRouterRequest()
  }

  def newRouterRequest() ={
    if(debug)println("Sending Id for new Router to Replicator")
    sender() ! AssignedId(roCounter)
    roCounter += 1
  }

  def newPMReqest() ={
    if(debug)println("Sending Id for new PM to Replicator")
    sender() ! AssignedId(pmCounter)
    pmCounter += 1
    if(debug)println("Sending new total Partition managers to all the subscribers")
    mediator ! DistributedPubSubMediator.Publish(Utils.partitionsTopic, PartitionsCount(pmCounter))}

  def keepAliveHandler() = {
    checkMapTime(PMKeepAlive)
    checkMapTime(RouterKeepAlive)
    if(!clusterUp)
      if(RouterKeepAlive.size>=minimumRouters)
        if(PMKeepAlive.size==managerCount){

          clusterUp=true

          if(debug) println("All Partition Managers and minimum number of routers have joined the cluster")
        }

  }

  def checkMapTime(map: TrieMap[Int,Long]) = map.foreach(pm =>
    if(pm._2 + maxTime <= System.currentTimeMillis())
      if(debug)println(s"Manager ${pm._1} not responding since ${Utils.unixToTimeStamp(pm._2)}"))

  def mapHandler(id: Int, map:TrieMap[Int,Long], mapType:String) = {
    if(debug)println(s"Inside map handler for $mapType $id")
    map.putIfAbsent(id,System.currentTimeMillis()) match {
      case Some(time) => map.update(id,System.currentTimeMillis())
      case _ => if(debug)println(s"$mapType $id has started sending keep alive messages at ${Utils.nowTimeStamp()}")
    }
  }

  def getManager(srcId:Int):String = s"/user/Manager_${srcId % managerCount}" //simple srcID hash at the moment


}

