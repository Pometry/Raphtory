package com.raphtory.core.actors.partitionmanager

import akka.actor.ActorRef

import scala.util.{Failure, Success}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.actors.RaphtoryActor
import com.raphtory.core.analysis.Analyser
import com.raphtory.core.model.communication._
import com.raphtory.core.storage.controller.GraphRepoProxy
import com.raphtory.core.utils.Utils
import com.twitter.util.Eval
import monix.execution.{ExecutionModel, Scheduler}

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future

class PartitionReader(id : Int, test : Boolean, managerCountVal : Int) extends RaphtoryActor {
  implicit var managerCount : Int = managerCountVal
  val managerID    : Int = id                   //ID which refers to the partitions position in the graph manager map
  implicit val s : Scheduler = Scheduler(ExecutionModel.BatchedExecution(1024))
  val mediator : ActorRef   = DistributedPubSub(context.system).mediator // get the mediator for sending cluster messages
  val analyserMap:TrieMap[String,Analyser] = TrieMap[String,Analyser]()

  mediator ! DistributedPubSubMediator.Put(self)
  mediator ! DistributedPubSubMediator.Subscribe(Utils.readersTopic, self)
  implicit val proxy = GraphRepoProxy
  override def preStart() = {
    println("Starting reader")
  }

  override def receive : Receive = {
      case Setup(analyzer) => setup(analyzer)
      case SetupNewAnalyser(analyser,name) =>setupNewAnalyser(analyser,name)
      case NextStep(analyzer) => nextStep(analyzer)
      case NextStepNewAnalyser(name) => nextStepNewAnalyser(name)
      case GetNetworkSize() => sender() ! NetworkSize(GraphRepoProxy.getVerticesSet().size)
      case UpdatedCounter(newValue) => managerCount = newValue
      case e => println(s"[READER] not handled message " + e)
  }

  def setup(analyzer:Analyser) {
    try {
      println("Setup analyzer, sending Ready packet")
      analyzer.sysSetup()
      analyzer.setup()
      sender() ! Ready()
    }
    catch {
      case e: ClassNotFoundException =>{
        println("Analyser not found within this image, requesting scala file")
        sender() ! ClassMissing
      }
    }
  }

  def setupNewAnalyser(analyserString: String, name: String) ={
    println(s"Received $name from LAM, compiling")
    try {
      val eval = new Eval // Initializing The Eval without any target location
      val analyser: Analyser = eval[Analyser](analyserString)
      analyserMap += ((name,analyser))
      setup(analyser)
    }
    catch {
      case e: Exception =>{
        sender() ! FailedToCompile(e.getStackTrace.toString)
      }
    }
  }


  def nextStep(analyzer: Analyser): Unit ={
    try{
      println(s"Received new step for pm_$managerID")
      analyzer.sysSetup()
      val senderPath = sender().path

      println("Inside future")
      val value = analyzer.analyse()

      println("StepEnd success. Sending to " + senderPath.toStringWithoutAddress)
      println(value)
      mediator ! DistributedPubSubMediator.Send(senderPath.toStringWithoutAddress, EndStep(value), false)
    }
    catch {
      case e: Exception =>{println(e.getStackTrace)}
    }
  }

  def nextStepNewAnalyser(name: String) = {
    nextStep(analyserMap(name))
  }
}

