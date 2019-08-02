package com.raphtory.core.components.PartitionManager.Workers

import akka.actor.{Actor, ActorPath, ActorRef}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.analysis.{Analyser, GraphRepoProxy, ManagerCount, WorkerID}
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
    case Setup(analyzer,jobID,superStep) => setup(analyzer,jobID,superStep)
    case CheckMessages(superstep) => checkMessages()
    case NextStep(analyzer,jobID,superStep) => nextStep(analyzer,jobID,superStep)
    case NextStepNewAnalyser(name,jobID,currentStep) => nextStepNewAnalyser(name,jobID,currentStep)
    case handler:MessageHandler => receivedMessage(handler)
  }

  def receivedMessage(handler:MessageHandler) = {
    receivedMessages.add(1)
    EntityStorage.vertices(handler.vertexID).vertexMultiQueue.receiveMessage(handler)
  }

  def checkMessages() ={
    var count = 0
    tempProxy.getVerticesSet()(WorkerID(workerId)).foreach(v => count += tempProxy.getVertex(v)(context,ManagerCount(1)).messageQueue2.size)
    var count2 = 0
    tempProxy.getVerticesSet()(WorkerID(workerId)).foreach(v => count2 += tempProxy.getVertex(v)(context,ManagerCount(1)).messageQueue.size)
    //println(s"queue next $count queue now $count2")
    sender() ! MessagesReceived(workerId,count,receivedMessages.get,tempProxy.getMessages())
  }

  def setup(analyzer: Analyser,jobID:String,superStep:Int) {
    EntityStorage.vertexKeys(workerId).foreach(v=> EntityStorage.vertices(v).vertexMultiQueue.clearQueues(jobID))
    receivedMessages.set(0)
    tempProxy = new GraphRepoProxy(jobID,superStep)
    analyzer.sysSetup(context,ManagerCount(managerCount),tempProxy)
    analyzer.setup()(new WorkerID(workerId))
    sender() ! Ready(tempProxy.getMessages())

  }

  def nextStep(analyzer: Analyser,jobID:String,superStep:Int): Unit = {
    //println(analyzer)
    receivedMessages.set(0)
    tempProxy = new GraphRepoProxy(jobID,superStep)
    analyzer.sysSetup(context,ManagerCount(managerCount),tempProxy)
    val value = analyzer.analyse()(new WorkerID(workerId))
    sender() ! EndStep(value,tempProxy.getMessages(),tempProxy.checkVotes(workerId))

  }

  def nextStepNewAnalyser(name: String,jobID:String,currentStep:Int) = {
    nextStep(Utils.analyserMap(name),jobID,currentStep)
  }

}
