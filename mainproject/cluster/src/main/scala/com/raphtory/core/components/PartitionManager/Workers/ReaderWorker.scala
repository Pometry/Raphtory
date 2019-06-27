package com.raphtory.core.components.PartitionManager.Workers

import akka.actor.{Actor, ActorPath, ActorRef}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.analysis.{Analyser, GraphRepoProxy}
import com.raphtory.core.model.communication._
import com.raphtory.core.utils.Utils

class ReaderWorker(managerCountVal:Int,managerID:Int,workerId:Int)  extends Actor{
  implicit val proxy = GraphRepoProxy
  implicit var managerCount: Int = managerCountVal
  val mediator: ActorRef = DistributedPubSub(context.system).mediator // get the mediator for sending cluster messages
  mediator ! DistributedPubSubMediator.Put(self)
  mediator ! DistributedPubSubMediator.Subscribe(Utils.readersWorkerTopic, self)
  val debug = false
  implicit val workerID: Int = workerId


  override def receive: Receive = {
    case UpdatedCounter(newValue) => managerCount = newValue
    case Setup(analyzer) => setup(analyzer)
    case NextStep(analyzer) => nextStep(analyzer)
    case NextStepNewAnalyser(name) => nextStepNewAnalyser(name)
  }

  def setup(analyzer: Analyser) {
    analyzer.sysSetup(context,managerCount,workerID.toShort,GraphRepoProxy)
    analyzer.setup()
    sender() ! Ready()
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
      analyzer.sysSetup(context,managerCount,workerID.toShort,GraphRepoProxy)
      val senderPath = sender().path
      this.analyze(analyzer, senderPath)
    }
    catch {
      case e: Exception => {
        sender() ! ExceptionInAnalysis(e.toString)
      }
    }
  }

  def nextStepNewAnalyser(name: String) = {
    nextStep(Utils.analyserMap(name))
  }


}
