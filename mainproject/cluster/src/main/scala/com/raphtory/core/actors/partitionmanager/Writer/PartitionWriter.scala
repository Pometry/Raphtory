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

  var childMap              : ParTrieMap[Int,ActorRef] = ParTrieMap[Int,ActorRef]()
  val children              : Int = 10
  val logChild              : ActorRef = context.actorOf(Props[LoggingSlave],s"logger")
  val mediator              : ActorRef = DistributedPubSub(context.system).mediator // get the mediator for sending cluster messages
  mediator ! DistributedPubSubMediator.Put(self)

  val storage= EntityStorage.apply(printing, managerCount, managerID, mediator)
  println(akka.serialization.Serialization.serializedActorPath(self))
  /**
    * Set up partition to report how many messages it has processed in the last X seconds
    */
  override def preStart() {
    println("starting writer")
    context.system.scheduler.schedule(Duration(10, SECONDS), Duration(10, SECONDS), self, "tick")
    context.system.scheduler.schedule(Duration(8, SECONDS), Duration(10, SECONDS), self, "keep_alive")

     for(i <- 0 to children){ //create threads for writing
       childMap.put(i,context.system.actorOf(Props(new WritingSlave(i)),s"Manager_${managerID}_child_$i"))
     }
   }

  override def receive : Receive = {
    //Logging block
    case "tick"                                                           => log()
    //misc and startup block
    case UpdatedCounter(newValue)                                         => {managerCount = newValue; storage.setManagerCount(managerCount)}
    case "keep_alive"                                                     =>  mediator ! DistributedPubSubMediator.Send("/user/WatchDog", PartitionUp(managerID), localAffinity = false)
    case e => println(s"Not handled message ${e.getClass} ${e.toString}")

    case EdgeUpdateProperty(msgTime, edgeId, key, value)                  => storage.updateEdgeProperties(msgTime, edgeId, key, value)   //for data coming from the LAM
    //case LiveAnalysis(name,analyser)                                      => mediator ! DistributedPubSubMediator.Send(name, Results(analyser.analyse(vertices,edges)), false)
 }

  def log() = {
    val messageCount = storage.messageCount.get()
    storage.messageCount.set(0)
    val secondaryMessageCount = storage.secondaryMessageCount.get()
    storage.secondaryMessageCount.set(0)
    logChild  ! ReportIntake(messageCount,secondaryMessageCount,managerID)
  }


}
