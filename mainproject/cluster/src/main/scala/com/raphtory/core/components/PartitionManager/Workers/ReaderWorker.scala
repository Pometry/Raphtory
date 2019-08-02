package com.raphtory.core.components.PartitionManager.Workers

import akka.actor.{Actor, ActorPath, ActorRef}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.analysis._
import com.raphtory.core.model.communication._
import com.raphtory.core.storage.EntityStorage
import com.raphtory.core.utils.Utils
import monix.execution.atomic.AtomicInt

class ReaderWorker(managerCountVal:Int,managerID:Int,workerId:Int)  extends Actor{
  implicit var managerCount: Int = managerCountVal
  val mediator: ActorRef = DistributedPubSub(context.system).mediator // get the mediator for sending cluster messages
  mediator ! DistributedPubSubMediator.Put(self)
  mediator ! DistributedPubSubMediator.Subscribe(Utils.readersWorkerTopic, self)
  var receivedMessages = AtomicInt(0)
  var tempProxy:GraphRepoProxy = null

  override def receive: Receive = {
    case UpdatedCounter(newValue) => managerCount = newValue

    case Setup(analyzer,jobID,superStep,timestamp) => setup(analyzer,jobID,superStep,timestamp)
    case CheckMessages(superstep) => checkMessages()
    case NextStep(analyzer,jobID,superStep,timestamp) => nextStep(analyzer,jobID,superStep,timestamp)
    case NextStepNewAnalyser(name,jobID,currentStep,timestamp) => nextStepNewAnalyser(name,jobID,currentStep,timestamp)
    case handler:MessageHandler => receivedMessage(handler)
  }

  def receivedMessage(handler:MessageHandler) = {
    receivedMessages.add(1)
    EntityStorage.vertices(handler.vertexID).mutliQueue.receiveMessage(handler)
  }

  def checkMessages() ={
    var count = 0
    tempProxy.getVerticesSet()(WorkerID(workerId)).foreach(v => count += tempProxy.getVertex(v)(context,ManagerCount(1)).messageQueue2.size)
    sender() ! MessagesReceived(workerId,count,receivedMessages.get,tempProxy.getMessages())
  }

  def setup(analyzer: Analyser,jobID:String,superStep:Int,timestamp:Long) {
    receivedMessages.set(0)
    setProxy(jobID,superStep,timestamp)
    EntityStorage.vertexKeys(workerId).foreach(v=> EntityStorage.vertices(v).mutliQueue.clearQueues(tempProxy.job()))
    analyzer.sysSetup(context,ManagerCount(managerCount),tempProxy)
    analyzer.setup()(new WorkerID(workerId))
    sender() ! Ready(tempProxy.getMessages())
  }

  def nextStep(analyzer: Analyser,jobID:String,superStep:Int,timestamp:Long): Unit = {
    //println(analyzer)
    receivedMessages.set(0)
    setProxy(jobID,superStep,timestamp)
    analyzer.sysSetup(context,ManagerCount(managerCount),tempProxy)
    val value = analyzer.analyse()(new WorkerID(workerId))
    sender() ! EndStep(value,tempProxy.getMessages(),tempProxy.checkVotes(workerId))

  }

  def nextStepNewAnalyser(name: String,jobID:String,currentStep:Int,timestamp:Long) = {
    nextStep(Utils.analyserMap(name),jobID,currentStep,timestamp)
  }

  private def setProxy(jobID:String,superStep:Int,timestamp:Long):Unit = {
    if(timestamp == -1)
      tempProxy = new GraphRepoProxy(jobID,superStep)
    else
      tempProxy = new GraphViewProxy(jobID,superStep,timestamp,WorkerID(workerId))
  }

}
