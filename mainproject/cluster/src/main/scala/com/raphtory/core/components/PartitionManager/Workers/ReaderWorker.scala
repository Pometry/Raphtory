package com.raphtory.core.components.PartitionManager.Workers

import akka.actor.{Actor, ActorPath, ActorRef}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.analysis.{Analyser, GraphRepoProxy, ManagerCount, WorkerID}
import com.raphtory.core.model.communication._
import com.raphtory.core.storage.EntityStorage
import com.raphtory.core.utils.Utils
import com.raphtory.examples.GenericAlgorithms.ExamplePageRank

class ReaderWorker(managerCountVal:Int,managerID:Int,workerId:Int)  extends Actor{
  implicit var managerCount: Int = managerCountVal
  val mediator: ActorRef = DistributedPubSub(context.system).mediator // get the mediator for sending cluster messages
  mediator ! DistributedPubSubMediator.Put(self)
  mediator ! DistributedPubSubMediator.Subscribe(Utils.readersWorkerTopic, self)
  val debug = false
  implicit val workerID: Int = workerId
  var receivedMessages = 0

  override def receive: Receive = {
    case UpdatedCounter(newValue) => managerCount = newValue
    case Setup(analyzer,jobID,superStep) => setup(analyzer,jobID,superStep)
    case CheckMessages() => sender() ! MessagesReceived(receivedMessages)
    case NextStep(analyzer,jobID,superStep) => nextStep(analyzer,jobID,superStep)
    case NextStepNewAnalyser(name,jobID,currentStep) => nextStepNewAnalyser(name,jobID,currentStep)
    case handler:MessageHandler => {
      receivedMessages+=1
      EntityStorage.vertices(handler.vertexID).vertexMultiQueue.receiveMessage(handler)
    }
  }

  def setup(analyzer: Analyser,jobID:String,superStep:Int) {
    val repo = new GraphRepoProxy(jobID,superStep)
    val analyzer2 = new ExamplePageRank(1,0)
    analyzer2.sysSetup(context,ManagerCount(managerCount),repo)
    analyzer2.setup()(new WorkerID(workerID))
    sender() ! Ready(repo.getMessages())
  }

  private def analyze(analyzer: Analyser, senderPath: ActorPath,messages:Int) = {
    val value = analyzer.analyse()(new WorkerID(workerID))
    //    val value
    if(debug)println("StepEnd success. Sending to " + senderPath.toStringWithoutAddress)
    if(debug)println(value)
    mediator ! DistributedPubSubMediator.Send(senderPath.toStringWithoutAddress, EndStep(value,messages), false)

  }

  def nextStep(analyzer: Analyser,jobID:String,superStep:Int): Unit = {
    try {
      receivedMessages=0
      if(debug)println(s"Received new step for pm_$managerID")
      val repo = new GraphRepoProxy(jobID,superStep)
      val analyzer2 = new ExamplePageRank(1,0)
      analyzer2.sysSetup(context,ManagerCount(managerCount),repo)
      val senderPath = sender().path
      this.analyze(analyzer2, senderPath,repo.getMessages())
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
