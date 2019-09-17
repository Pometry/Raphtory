package com.raphtory.core.components.PartitionManager.Workers

import akka.actor.{Actor, ActorPath, ActorRef}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.analysis.API.GraphRepositoryProxies.{LiveProxy, ViewProxy, WindowProxy}
import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.analysis.API._
import com.raphtory.core.model.communication._
import com.raphtory.core.storage.EntityStorage
import com.raphtory.core.utils.Utils
import monix.execution.atomic.AtomicInt

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.ExecutionContext.Implicits.global

class ReaderWorker(managerCountVal:Int,managerID:Int,workerId:Int)  extends Actor{
  implicit var managerCount: Int = managerCountVal
  val mediator: ActorRef = DistributedPubSub(context.system).mediator // get the mediator for sending cluster messages
  mediator ! DistributedPubSubMediator.Put(self)
  mediator ! DistributedPubSubMediator.Subscribe(Utils.readersWorkerTopic, self)
  var receivedMessages = AtomicInt(0)
  var tempProxy:LiveProxy = null

  override def receive: Receive = {
    case UpdatedCounter(newValue) => managerCount = newValue
    case Setup(analyzer,jobID,superStep,timestamp,analysisType,window,windowSet) => setup(analyzer,jobID,superStep,timestamp,analysisType,window,windowSet)
    case CheckMessages(superstep) => checkMessages()
    case NextStep(analyzer,jobID,superStep,timestamp,analysisType,window,windowSet) => nextStep(analyzer,jobID,superStep,timestamp,analysisType,window,windowSet)
    case NextStepNewAnalyser(name,jobID,currentStep,timestamp,analysisType,window,windowSet) => nextStepNewAnalyser(name,jobID,currentStep,timestamp,analysisType,window,windowSet)
    case handler:MessageHandler => receivedMessage(handler)
  }

  def receivedMessage(handler:MessageHandler) = {
    receivedMessages.add(1)
    EntityStorage.vertices(workerId)(handler.vertexID).mutliQueue.receiveMessage(handler)
  }

  def checkMessages() ={
    var count = 0
    tempProxy.getVerticesSet().foreach(v => count += tempProxy.getVertex(v)(context,ManagerCount(1)).messageQueue2.size)
    sender() ! MessagesReceived(workerId,count,receivedMessages.get,tempProxy.getMessages())
  }

  def setup(analyzer: Analyser,jobID:String,superStep:Int,timestamp:Long,analysisType:AnalysisType.Value,window:Long,windowSet:Array[Long]) {
    receivedMessages.set(0)
    //val setProx = System.currentTimeMillis()
    setProxy(jobID,superStep,timestamp,analysisType,window,windowSet)
    //println(s"$workerId took ${System.currentTimeMillis()-setProx} to setProxy}")
    //val queueClear = System.currentTimeMillis()
    EntityStorage.vertices(workerId).foreach(v=> (v._2).mutliQueue.clearQueues(tempProxy.job()))
    //println(s"$workerId took ${System.currentTimeMillis()-queueClear} to clear queue}")
    analyzer.sysSetup(context,ManagerCount(managerCount),tempProxy,workerId)
    if(windowSet.isEmpty) {
      analyzer.setup()
      sender() ! Ready(tempProxy.getMessages())
    }
    else{
      //val individualResults:mutable.HashMap[Long,Any] = mutable.HashMap[Long,Any]()
      analyzer.setup()
      var currentWindow = 1
      while(currentWindow<windowSet.size){
        tempProxy.asInstanceOf[WindowProxy].shrinkWindow(windowSet(currentWindow))
        analyzer.setup()
        currentWindow +=1
      }
      sender() ! Ready(tempProxy.getMessages())
    }
  }

  def nextStep(analyzer: Analyser,jobID:String,superStep:Int,timestamp:Long,analysisType:AnalysisType.Value,window:Long,windowSet:Array[Long]): Unit = {
    receivedMessages.set(0)
    //val setProx = System.currentTimeMillis()
    setProxy(jobID,superStep,timestamp,analysisType,window,windowSet)
    //println(s"$workerId took ${System.currentTimeMillis()-setProx} to setProxy in next step}")
    analyzer.sysSetup(context,ManagerCount(managerCount),tempProxy,workerId)
    if(windowSet.isEmpty) {
      val value = analyzer.analyse()
      sender() ! EndStep(value,tempProxy.getMessages(),tempProxy.checkVotes(workerId))
    }
    else{
      val individualResults:mutable.ArrayBuffer[Any] = ArrayBuffer[Any]()
      //val analysisTime = System.currentTimeMillis()
      individualResults += analyzer.analyse()
      //println(s"$workerId took ${System.currentTimeMillis()-analysisTime} to analyise in next step}")
      for(i<- windowSet.indices)
        if(i!=0) {
          tempProxy.asInstanceOf[WindowProxy].shrinkWindow(windowSet(i))
          individualResults += analyzer.analyse()
        }
      sender() ! EndStep(individualResults,tempProxy.getMessages(),tempProxy.checkVotes(workerId))
      }

  }

  def nextStepNewAnalyser(name: String,jobID:String,currentStep:Int,timestamp:Long,analysisType:AnalysisType.Value,window:Long,windowSet:Array[Long]) = {
    nextStep(Utils.analyserMap(name),jobID,currentStep,timestamp,analysisType,window,windowSet)
  }

  private def setProxy(jobID:String,superStep:Int,timestamp:Long,analysisType:AnalysisType.Value,window:Long,windowSet:Array[Long]):Unit = {
    analysisType match {
      case AnalysisType.live => {
        if(windowSet.nonEmpty) //we have a set of windows to run
          tempProxy = new WindowProxy(jobID,superStep,EntityStorage.newestTime,windowSet(0),WorkerID(workerId))
        else if(window != -1) // we only have one window to run
          tempProxy = new WindowProxy(jobID,superStep,EntityStorage.newestTime,window,WorkerID(workerId))
        else
          tempProxy = new LiveProxy(jobID,superStep,timestamp,window,WorkerID(workerId))
      }
      case AnalysisType.view  => {
        if(windowSet.nonEmpty) //we have a set of windows to run
          tempProxy = new WindowProxy(jobID,superStep,timestamp,windowSet(0),WorkerID(workerId))
        else if(window != -1) // we only have one window to run
          tempProxy = new WindowProxy(jobID,superStep,timestamp,window,WorkerID(workerId))
        else
          tempProxy = new ViewProxy(jobID,superStep,timestamp,WorkerID(workerId))
      }
      case AnalysisType.range  => {
        if(windowSet.nonEmpty) //we have a set of windows to run
          tempProxy = new WindowProxy(jobID,superStep,timestamp,windowSet(0),WorkerID(workerId))
        else if(window != -1) // we only have one window to run
          tempProxy = new WindowProxy(jobID,superStep,timestamp,window,WorkerID(workerId))
        else
          tempProxy = new ViewProxy(jobID,superStep,timestamp,WorkerID(workerId))
      }
    }
  }

}
