package com.raphtory.core.components.PartitionManager.Workers

import akka.actor.{Actor, ActorPath, ActorRef}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.analysis.{Analyser, GraphRepoProxy, ManagerCount, Worker}
import com.raphtory.core.model.communication._
import com.raphtory.core.storage.EntityStorage
import com.raphtory.core.utils.Utils

class ReaderWorker(managerCountVal:Int,managerID:Int,workerId:Int)  extends Actor{
  implicit var managerCount: Int = managerCountVal
  val mediator: ActorRef = DistributedPubSub(context.system).mediator // get the mediator for sending cluster messages
  mediator ! DistributedPubSubMediator.Put(self)
  mediator ! DistributedPubSubMediator.Subscribe(Utils.readersWorkerTopic, self)
  val debug = false
  implicit val workerID: Int = workerId


  override def receive: Receive = {
    case UpdatedCounter(newValue) => managerCount = newValue
    case Setup(analyzer,jobID,superStep) => setup(analyzer,jobID,superStep)
    case NextStep(analyzer,jobID,superStep) => nextStep(analyzer,jobID,superStep)
    case NextStepNewAnalyser(name,jobID,currentStep) => nextStepNewAnalyser(name,jobID,currentStep)
    case handler:MessageHandler => EntityStorage.vertices(handler.vertexID).vertexMultiQueue.receiveMessage(handler)
  }

  def setup(analyzer: Analyser,jobID:String,superStep:Int) {
    analyzer.sysSetup(context,ManagerCount(managerCount),new GraphRepoProxy(jobID,superStep))
    analyzer.setup()(new Worker(workerID))
    sender() ! Ready()
  }

  private def analyze(analyzer: Analyser, senderPath: ActorPath) = {

    val value = analyzer.analyse()(new Worker(workerID))
    if(debug)println("StepEnd success. Sending to " + senderPath.toStringWithoutAddress)
    if(debug)println(value)
    mediator ! DistributedPubSubMediator.Send(senderPath.toStringWithoutAddress, EndStep(value), false)

  }

  def nextStep(analyzer: Analyser,jobID:String,superStep:Int): Unit = {
    try {
      if(debug)println(s"Received new step for pm_$managerID")
      analyzer.sysSetup(context,ManagerCount(managerCount),new GraphRepoProxy(jobID,superStep))
      val senderPath = sender().path
      this.analyze(analyzer, senderPath)
    }
    catch {
      case e: Exception => {
        sender() ! ExceptionInAnalysis(e.toString)
      }
    }
  }

  def nextStepNewAnalyser(name: String,jobID:String,currentStep:Int) = {
    nextStep(Utils.analyserMap(name),jobID,currentStep)
  }


}
