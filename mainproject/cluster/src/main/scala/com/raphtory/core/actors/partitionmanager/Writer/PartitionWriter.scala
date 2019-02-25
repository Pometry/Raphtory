package com.raphtory.core.actors.partitionmanager.Writer

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ActorRef, Props}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.actors.RaphtoryActor
import com.raphtory.core.actors.partitionmanager.Writer.Helpers.{LoggingSlave, WritingSlave}
import com.raphtory.core.model.communication._
import com.raphtory.core.model.graphentities.Entity
import com.raphtory.core.storage.EntityStorage


import scala.concurrent.ExecutionContext.Implicits.global

import scala.collection.parallel.mutable.ParTrieMap
import scala.concurrent.duration._

/**
  * The graph partition manages a set of vertices and there edges
  * Is sent commands which have been processed by the command Processor
  * Will process these, storing information in graph entities which may be updated if they already exist
  * */
class PartitionWriter(id : Int, test : Boolean, managerCountVal : Int) extends RaphtoryActor {
  var managerCount          : Int = managerCountVal
  val managerID             : Int = id                   //ID which refers to the partitions position in the graph manager map

  val printing              : Boolean = false                  // should the handled messages be printed to terminal

  var messageCount          : Int = 0        // number of messages processed since last report to the benchmarker
  var secondaryMessageCount : Int = 0

  var childMap              : ParTrieMap[Int,ActorRef] = ParTrieMap[Int,ActorRef]()
  val children              : Int = 10
  val logChild              : ActorRef = context.actorOf(Props[LoggingSlave],s"logger")
  val mediator              : ActorRef = DistributedPubSub(context.system).mediator // get the mediator for sending cluster messages
  mediator ! DistributedPubSubMediator.Put(self)

  val storage= EntityStorage.apply(printing, managerCount, managerID, mediator)

  /**
    * Set up partition to report how many messages it has processed in the last X seconds
    */
  override def preStart() {
    println("starting writer")
    context.system.scheduler.schedule(Duration(10, SECONDS),
      Duration(1, SECONDS), self, "tick")
    context.system.scheduler.schedule(Duration(8, SECONDS),
      Duration(10, SECONDS), self, "keep_alive")

     for(i <- 0 to children){ //create threads for writing
       childMap.put(i,context.actorOf(Props[WritingSlave],s"child_$i"))
     }
   }

  override def receive : Receive = {

    //Forwarding of writes to writing slaves
    case VertexAdd(routerID,msgTime,srcId)                                => { getChild() ! VertexAdd(routerID,msgTime,srcId); vHandle(srcId,msgTime)}
    case VertexRemoval(routerID,msgTime,srcId)                            => { getChild() ! VertexRemoval(routerID,msgTime,srcId); vHandle(srcId,msgTime)}
    case VertexAddWithProperties(routerID,msgTime,srcId,properties)       => { getChild() ! VertexAddWithProperties(routerID,msgTime,srcId,properties); vHandle(srcId,msgTime)}

    case EdgeAdd(routerID,msgTime,srcId,dstId)                            => { getChild() ! EdgeAdd(routerID,msgTime,srcId,dstId); eHandle(srcId,dstId,msgTime)}
    case RemoteEdgeAdd(routerID,msgTime,srcId,dstId,properties)           => { getChild() ! RemoteEdgeAdd(routerID,msgTime,srcId,dstId,properties);eHandleSecondary(srcId,dstId,msgTime)}
    case RemoteEdgeAddNew(routerID,msgTime,srcId,dstId,properties,deaths) => { getChild() ! RemoteEdgeAddNew(routerID,msgTime,srcId,dstId,properties,deaths);eHandleSecondary(srcId,dstId,msgTime)}
    case EdgeAddWithProperties(routerID,msgTime,srcId,dstId,properties)   => { getChild() ! EdgeAddWithProperties(routerID,msgTime,srcId,dstId,properties);eHandle(srcId,dstId,msgTime)}

    case EdgeRemoval(routerID,msgTime,srcId,dstId)                        => { getChild() ! EdgeRemoval(routerID,msgTime,srcId,dstId);eHandle(srcId,dstId,msgTime)}
    case RemoteEdgeRemoval(routerID,msgTime,srcId,dstId)                  => { getChild() ! RemoteEdgeRemoval(routerID,msgTime,srcId,dstId);eHandleSecondary(srcId,dstId,msgTime)}
    case RemoteEdgeRemovalNew(routerID,msgTime,srcId,dstId,deaths)        => { getChild() ! RemoteEdgeRemovalNew(routerID,msgTime,srcId,dstId,deaths);eHandleSecondary(srcId,dstId,msgTime)}

    case ReturnEdgeRemoval(routerID,msgTime,srcId,dstId)                  => { getChild() ! ReturnEdgeRemoval(routerID,msgTime,srcId,dstId);eHandleSecondary(srcId,dstId,msgTime)}
    case RemoteReturnDeaths(msgTime,srcId,dstId,deaths)                   => { getChild() ! RemoteReturnDeaths(msgTime,srcId,dstId,deaths);eHandleSecondary(srcId,dstId,msgTime)}

    //Logging block
    case "tick"                                                           => {logChild  ! ReportIntake(messageCount,secondaryMessageCount,managerID); resetCounters()}


    //misc and startup block
    case UpdatedCounter(newValue)                                         => {managerCount = newValue; storage.setManagerCount(managerCount)}
    case "keep_alive"                                                     =>  mediator ! DistributedPubSubMediator.Send("/user/WatchDog", PartitionUp(managerID), localAffinity = false)
    case e => println(s"Not handled message ${e.getClass} ${e.toString}")

    case EdgeUpdateProperty(msgTime, edgeId, key, value)                  => storage.updateEdgeProperties(msgTime, edgeId, key, value)   //for data coming from the LAM
    //case LiveAnalysis(name,analyser)                                      => mediator ! DistributedPubSubMediator.Send(name, Results(analyser.analyse(vertices,edges)), false)
 }

  def getChild():ActorRef = childMap.getOrElse((messageCount+1) % children, null)

  def resetCounters() = {
    messageCount = 0
    secondaryMessageCount = 0
  }
  def vHandle(srcID : Int,msgTime:Long) : Unit = {
    storage.timings(msgTime)
    messageCount += 1
  }

  def vHandleSecondary(srcID : Int,msgTime:Long) : Unit = {
    storage.timings(msgTime)
    secondaryMessageCount +=1
  }
  def eHandle(srcID : Int, dstID : Int,msgTime:Long) : Unit = {
    storage.timings(msgTime)
    messageCount += 1
  }

  def eHandleSecondary(srcID : Int, dstID : Int,msgTime:Long) : Unit = {
    storage.timings(msgTime)
    secondaryMessageCount += 1
  }

}
