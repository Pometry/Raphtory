package com.raphtory.core.components.PartitionManager

import akka.actor.SupervisorStrategy.Resume
import akka.actor.{Actor, ActorRef, OneForOneStrategy, Props, Terminated}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.components.PartitionManager.Workers.WriterLogger
import com.raphtory.core.model.communication._
import com.raphtory.core.storage.EntityStorage

import scala.collection.parallel.mutable.ParTrieMap
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
  * The graph partition manages a set of vertices and there edges
  * Is sent commands which have been processed by the command Processor
  * Will process these, storing information in graph entities which may be updated if they already exist
  * */
class Writer(id : Int, test : Boolean, managerCountVal : Int, workers: ParTrieMap[Int,ActorRef],storage:ParTrieMap[Int,EntityStorage]) extends Actor {
  var managerCount          : Int = managerCountVal
  val managerID             : Int = id                   //ID which refers to the partitions position in the graph manager map

  val printing              : Boolean = false                  // should the handled messages be printed to terminal

  val children              : Int = 10
  val logChild              : ActorRef = context.actorOf(Props[WriterLogger].withDispatcher("logging-dispatcher"),s"logger")
  val logChildForSize       : ActorRef = context.actorOf(Props[WriterLogger].withDispatcher("logging-dispatcher"),s"logger2")
  val mediator              : ActorRef = DistributedPubSub(context.system).mediator // get the mediator for sending cluster messages
  var lastLogTime           = System.currentTimeMillis()/1000

  var messageCount = 0
  var secondaryMessageCount = 0
  var workerMessageCount = 0
  storage.foreach(s=> s._2.apply(printing,managerCount,managerID,mediator))

  mediator ! DistributedPubSubMediator.Put(self)

  println(akka.serialization.Serialization.serializedActorPath(self))
  /**
    * Set up partition to report how many messages it has processed in the last X seconds
    */
  override def supervisorStrategy = OneForOneStrategy() {
    case e: Exception => {e.printStackTrace(); Resume}
  }

  import scala.concurrent.duration.Duration

  override def preStart() {
    println("starting writer")
    context.system.scheduler.schedule(Duration(10, SECONDS), Duration(10, SECONDS), self, "log")
    context.system.scheduler.schedule(Duration(10, SECONDS), Duration(1, SECONDS), self, "count")
    context.system.scheduler.schedule(Duration(10, SECONDS), Duration(10, SECONDS), self, "keep_alive")


    println(context.children)
   }

  override def receive : Receive = {
    //Logging block
    case "log"                                                              => log()
    case "count"                                                            => count()
    case Terminated(child)                                                  => println(s"manager $managerID ${child.path} has died")
    //misc and startup block
    case UpdatedCounter(newValue)                                           => {managerCount = newValue; storage.foreach(s=> s._2.setManagerCount(managerCount))}
    case "keep_alive"                                                       =>  mediator ! DistributedPubSubMediator.Send("/user/WatchDog", PartitionUp(managerID), localAffinity = false)
    case e => println(s"Not handled message ${e.getClass} ${e.toString}")

//    case EdgeUpdateProperty(msgTime, edgeId, key, value)                  => storage.updateEdgeProperties(msgTime, edgeId, key, value)   //for data coming from the LAM
    //case LiveAnalysis(name,analyser)                                      => mediator ! DistributedPubSubMediator.Send(name, Results(analyser.analyse(vertices,edges)), false)
 }

  def log() = {
    logChildForSize ! ReportSize(managerID)
  }
  def count() = {
    val newTime = System.currentTimeMillis()/1000
    var timeDifference = (newTime-lastLogTime)
    if(timeDifference ==0) timeDifference =1
//    lastLogTime = newTime
//    var newMessageCount = EntityStorage.messageCount.sum
//    var newSecondaryMessageCount = EntityStorage.secondaryMessageCount.sum
//    var newWorkerMessageCount = EntityStorage.workerMessageCount.sum
//    logChild  ! ReportIntake((newMessageCount-messageCount),(newSecondaryMessageCount-secondaryMessageCount),(newWorkerMessageCount-workerMessageCount),managerID,timeDifference)
//    messageCount = newMessageCount
//    secondaryMessageCount = newSecondaryMessageCount
//    workerMessageCount = newWorkerMessageCount
  }


}
