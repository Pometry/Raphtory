package com.raphtory.core.actors.partitionmanager

import akka.actor.{ActorPath, ActorRef}

import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.actors.RaphtoryActor
import com.raphtory.core.analysis.Analyser
import com.raphtory.core.model.communication._
import com.raphtory.core.storage.controller.GraphRepoProxy
import com.raphtory.core.utils.Utils
import com.twitter.util.Eval
import scala.collection.concurrent.TrieMap
import monix.execution.{ExecutionModel, Scheduler}

class PartitionReader(id : Int, test : Boolean, managerCountVal : Int) extends RaphtoryActor {
  implicit var managerCount: Int = managerCountVal
  val managerID: Int = id //ID which refers to the partitions position in the graph manager map
  implicit val s: Scheduler = Scheduler(ExecutionModel.AlwaysAsyncExecution)
  val mediator: ActorRef = DistributedPubSub(context.system).mediator // get the mediator for sending cluster messages
  val analyserMap: TrieMap[String, Analyser] = TrieMap[String, Analyser]()

  mediator ! DistributedPubSubMediator.Put(self)
  mediator ! DistributedPubSubMediator.Subscribe(Utils.readersTopic, self)
  implicit val proxy = GraphRepoProxy

  override def preStart() = {
    println("Starting reader")
  }

  private def analyze(analyzer: Analyser, senderPath: ActorPath) = {
    val value = analyzer.analyse()
    println("StepEnd success. Sending to " + senderPath.toStringWithoutAddress)
    println(value)
    mediator ! DistributedPubSubMediator.Send(senderPath.toStringWithoutAddress, EndStep(value), false)
  }

  override def receive: Receive = {
    case Setup(analyzer) => setup(analyzer)
    case SetupNewAnalyser(analyser, name) => setupNewAnalyser(analyser, name)
    case NextStep(analyzer) => nextStep(analyzer)
    case NextStepNewAnalyser(name) => nextStepNewAnalyser(name)
    case GetNetworkSize() => sender() ! NetworkSize(GraphRepoProxy.getVerticesSet().size)
    case UpdatedCounter(newValue) => managerCount = newValue
    case e => println(s"[READER] not handled message " + e)
  }

  def nextStep(analyzer: Analyser): Unit = {
    try {
      println(s"Received new step for pm_$managerID")
      analyzer.sysSetup()
      val senderPath = sender().path
      this.analyze(analyzer, senderPath)

    }
    catch {
      case e: Exception => {
        println(e.getStackTrace)
      }
    }
  }

  def nextStepNewAnalyser(name: String) = {
    nextStep(analyserMap(name))
  }

  def setup(analyzer: Analyser) {
    try {
      //throw new ClassNotFoundException()
      analyzer.sysSetup()
      analyzer.setup()
      sender() ! Ready()
      println("Setup analyzer, sending Ready packet")
    }
    catch {
      case e: ClassNotFoundException => {
        println("Analyser not found within this image, requesting scala file")
        sender() ! ClassMissing()
      }
      case e: scala.NotImplementedError => {
        println("Analyser not found within this image, requesting scala file")
        sender() ! ClassMissing()
      }
    }
  }

  def setupNewAnalyser(analyserString: String, name: String) = {
    println(s"Received $name from LAM, compiling")
    try {
      val eval = new Eval // Initializing The Eval without any target location
      val analyser: Analyser = eval[Analyser](analyserString)
      analyserMap += ((name, analyser))
      println("before sys Setup")
      analyser.sysSetup()
      println("after sys setup")
      analyser.setup()
      println("after setup")
      sender() ! Ready()
    }
    catch {
      case e: Exception => {
        sender() ! FailedToCompile(e.getMessage)
      }
    }
  }
}

