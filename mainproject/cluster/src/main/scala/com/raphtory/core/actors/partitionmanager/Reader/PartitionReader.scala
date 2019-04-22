package com.raphtory.core.actors.partitionmanager.Reader

import akka.actor.{ActorPath, ActorRef}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.actors.RaphtoryActor
import com.raphtory.core.analysis.Analyser
import com.raphtory.core.model.communication._
import com.raphtory.core.storage.GraphRepoProxy
import com.raphtory.core.utils.Utils
import com.twitter.util.Eval
import monix.eval.Task
import monix.execution.Scheduler

import scala.collection.concurrent.TrieMap

class PartitionReader(id : Int, test : Boolean, managerCountVal : Int) extends RaphtoryActor {
  implicit var managerCount: Int = managerCountVal
  val managerID: Int = id //ID which refers to the partitions position in the graph manager map
  val mediator: ActorRef = DistributedPubSub(context.system).mediator // get the mediator for sending cluster messages
  val analyserMap: TrieMap[String, Analyser] = TrieMap[String, Analyser]()
  implicit val s = Scheduler.computation()

  mediator ! DistributedPubSubMediator.Put(self)
  mediator ! DistributedPubSubMediator.Subscribe(Utils.readersTopic, self)
  implicit val proxy = GraphRepoProxy

  val debug = false

  override def preStart() = {
   if(debug)println("Starting reader")
  }

  override def receive: Receive = {
    case AnalyserPresentCheck(classname) => presentCheck(classname)
    case Setup(analyzer) => setup(analyzer)
    case SetupNewAnalyser(analyser, name) => setupNewAnalyser(analyser, name)
    case NextStep(analyzer) => nextStep(analyzer)
    case NextStepNewAnalyser(name) => nextStepNewAnalyser(name)
    case GetNetworkSize() => sender() ! NetworkSize(GraphRepoProxy.getVerticesSet().size)
    case UpdatedCounter(newValue) => managerCount = newValue
    case e => println(s"[READER] not handled message " + e)
  }

  def presentCheck(classname:String) = {
    try {
      Class.forName(classname)
      if(debug)println(s"Reader has this class can precede: $classname ")
      sender() ! AnalyserPresent()
    }
    catch {
      case e: ClassNotFoundException => {
        if(debug)println("Analyser not found within this image, requesting scala file")
        sender() ! ClassMissing()
      }
    }
  }

  def setup(analyzer: Analyser) {
    try {
      //throw new ClassNotFoundException()
      analyzer.sysSetup()
      analyzer.setup()
      sender() ! Ready()
      if(debug)println("Setup analyzer, sending Ready packet")
    }
    catch {
      case e: ClassNotFoundException => {
        if(debug)println("Analyser not found within this image, requesting scala file")
        sender() ! ClassMissing()
      }
      //    case e: scala.NotImplementedError => {
      //      println("Analyser not found within this image, requesting scala file")
      //      sender() ! ClassMissing()
      //    }
    }
  }

  def setupNewAnalyser(analyserString: String, name: String) = {
    if(debug)println(s"Received $name from LAM, compiling")
    try {
      val eval = new Eval // Initializing The Eval without any target location
      val analyser: Analyser = eval[Analyser](analyserString)
      analyserMap += ((name, analyser))
      if(debug)println("before sys Setup")
      analyser.sysSetup()
      if(debug)println("after sys setup")
      analyser.setup()
      if(debug)println("after setup")
      sender() ! Ready()
    }
    catch {
      case e: Exception => {
        sender() ! FailedToCompile(e.getMessage)
      }
    }
  }

  private def analyze(analyzer: Analyser, senderPath: ActorPath) = {
    val value = analyzer.analyse()
    if(debug)println("StepEnd success. Sending to " + senderPath.toStringWithoutAddress)
    if(debug)println(value)
    mediator ! DistributedPubSubMediator.Send(senderPath.toStringWithoutAddress, EndStep(value), false)
  }





  def nextStep(analyzer: Analyser): Unit = {
    try {
      if(debug)println(s"Received new step for pm_$managerID")
      analyzer.sysSetup()
      val senderPath = sender().path
      Task.eval(this.analyze(analyzer, senderPath)).runAsync
    }
    catch {
      case e: Exception => {
        if(debug)println(e.getStackTrace)
      }
    }
  }

  def nextStepNewAnalyser(name: String) = {
    nextStep(analyserMap(name))
  }




}

