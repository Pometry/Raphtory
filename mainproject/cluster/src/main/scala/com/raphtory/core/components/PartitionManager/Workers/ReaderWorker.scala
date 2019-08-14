package com.raphtory.core.components.PartitionManager.Workers

import akka.actor.{Actor, ActorPath, ActorRef}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.analysis.GraphRepositoryProxies.{GraphProxy, ViewProxy, WindowProxy}
import com.raphtory.core.analysis._
import com.raphtory.core.model.communication._
import com.raphtory.core.storage.EntityStorage
import com.raphtory.core.utils.Utils
import monix.execution.atomic.AtomicInt

import scala.collection.mutable

class ReaderWorker(managerCountVal:Int,managerID:Int,workerId:Int)  extends Actor{
  implicit var managerCount: Int = managerCountVal
  val mediator: ActorRef = DistributedPubSub(context.system).mediator // get the mediator for sending cluster messages
  mediator ! DistributedPubSubMediator.Put(self)
  mediator ! DistributedPubSubMediator.Subscribe(Utils.readersWorkerTopic, self)
  var receivedMessages = AtomicInt(0)
  var tempProxy:GraphProxy = null

  override def receive: Receive = {
    case UpdatedCounter(newValue) => managerCount = newValue

    case Setup(analyzer,jobID,superStep,timestamp,window,windowSet) => setup(analyzer,jobID,superStep,timestamp,window,windowSet)
    case CheckMessages(superstep) => checkMessages()
    case NextStep(analyzer,jobID,superStep,timestamp,window,windowSet) => nextStep(analyzer,jobID,superStep,timestamp,window,windowSet)
    case NextStepNewAnalyser(name,jobID,currentStep,timestamp,window,windowSet) => nextStepNewAnalyser(name,jobID,currentStep,timestamp,window,windowSet)
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

  def setup(analyzer: Analyser,jobID:String,superStep:Int,timestamp:Long,window:Long,windowSet:Array[Long]) {
    receivedMessages.set(0)
    setProxy(jobID,superStep,timestamp,window,windowSet)
    EntityStorage.vertexKeys(workerId).foreach(v=> EntityStorage.vertices(v).mutliQueue.clearQueues(tempProxy.job()))
    analyzer.sysSetup(context,ManagerCount(managerCount),tempProxy)
    if(windowSet.isEmpty) {
      analyzer.setup()(new WorkerID(workerId))
      sender() ! Ready(tempProxy.getMessages())
    }
    else{
      //val individualResults:mutable.HashMap[Long,Any] = mutable.HashMap[Long,Any]()
      analyzer.setup()(new WorkerID(workerId))
      var currentWindow = 1
      while(currentWindow<windowSet.size){
        //filter
        analyzer.setup()(new WorkerID(workerId))
        currentWindow +=1
      }
    }
  }

  def nextStep(analyzer: Analyser,jobID:String,superStep:Int,timestamp:Long,window:Long,windowSet:Array[Long]): Unit = {
    //println(analyzer)
    receivedMessages.set(0)
    setProxy(jobID,superStep,timestamp,window,windowSet)
    analyzer.sysSetup(context,ManagerCount(managerCount),tempProxy)
    val value = analyzer.analyse()(new WorkerID(workerId))
    sender() ! EndStep(value,tempProxy.getMessages(),tempProxy.checkVotes(workerId))

  }

  def nextStepNewAnalyser(name: String,jobID:String,currentStep:Int,timestamp:Long,window:Long,windowSet:Array[Long]) = {
    nextStep(Utils.analyserMap(name),jobID,currentStep,timestamp,window,windowSet)
  }

  private def setProxy(jobID:String,superStep:Int,timestamp:Long,window:Long,windowSet:Array[Long]):Unit = {
    if(windowSet.nonEmpty) //we have a set of windows to run
      tempProxy = new WindowProxy(jobID,superStep,timestamp,windowSet(0),WorkerID(workerId))
    else if(window != -1) // we only have one window to run
      tempProxy = new WindowProxy(jobID,superStep,timestamp,window,WorkerID(workerId))
    else if (timestamp != -1) //we are only interested in a singular view
      tempProxy = new ViewProxy(jobID,superStep,timestamp,WorkerID(workerId))
    else //we are running on the live graph
      tempProxy = new GraphProxy(jobID,superStep,timestamp,window)

  }

}
