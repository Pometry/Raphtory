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

class ReaderWorker(managerCountVal:Int,managerID:Int,workerId:Int)  extends Actor{
  implicit var managerCount: Int = managerCountVal
  val mediator: ActorRef = DistributedPubSub(context.system).mediator // get the mediator for sending cluster messages
  mediator ! DistributedPubSubMediator.Put(self)
  mediator ! DistributedPubSubMediator.Subscribe(Utils.readersWorkerTopic, self)
  var receivedMessages = AtomicInt(0)
  var tempProxy:LiveProxy = null

  override def receive: Receive = {
    case UpdatedCounter(newValue) => managerCount = newValue

    case Setup(analyzer,jobID,superStep,timestamp,analysisType,window,windowSet) => try{setup(analyzer,jobID,superStep,timestamp,analysisType,window,windowSet)}catch {case e:Exception => self ! Setup(analyzer,jobID,superStep,timestamp,analysisType,window,windowSet)}
    case CheckMessages(superstep) => try{checkMessages() }catch {case e:Exception => self ! CheckMessages(superstep)}
    case NextStep(analyzer,jobID,superStep,timestamp,analysisType,window,windowSet) => try{nextStep(analyzer,jobID,superStep,timestamp,analysisType,window,windowSet) }catch {case e:Exception => self !NextStep(analyzer,jobID,superStep,timestamp,analysisType,window,windowSet)}
    case NextStepNewAnalyser(name,jobID,currentStep,timestamp,analysisType,window,windowSet) => nextStepNewAnalyser(name,jobID,currentStep,timestamp,analysisType,window,windowSet)
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

  def setup(analyzer: Analyser,jobID:String,superStep:Int,timestamp:Long,analysisType:AnalysisType.Value,window:Long,windowSet:Array[Long]) {
    receivedMessages.set(0)
    setProxy(jobID,superStep,timestamp,analysisType,window,windowSet)
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
        tempProxy.asInstanceOf[WindowProxy].shrinkWindow(windowSet(currentWindow))
        analyzer.setup()(new WorkerID(workerId))
        currentWindow +=1
      }
      sender() ! Ready(tempProxy.getMessages())
    }

  }

  def nextStep(analyzer: Analyser,jobID:String,superStep:Int,timestamp:Long,analysisType:AnalysisType.Value,window:Long,windowSet:Array[Long]): Unit = {
    //println(analyzer)
    receivedMessages.set(0)
    setProxy(jobID,superStep,timestamp,analysisType,window,windowSet)
    analyzer.sysSetup(context,ManagerCount(managerCount),tempProxy)
    if(windowSet.isEmpty) {
      val value = analyzer.analyse()(new WorkerID(workerId))
      sender() ! EndStep(value,tempProxy.getMessages(),tempProxy.checkVotes(workerId))
    }
    else{
      val individualResults:mutable.ArrayBuffer[Any] = ArrayBuffer[Any]()
      individualResults += analyzer.analyse()(new WorkerID(workerId))
      var currentWindow = 1
      while(currentWindow<windowSet.size) {
        tempProxy.asInstanceOf[WindowProxy].shrinkWindow(windowSet(currentWindow))
        individualResults += analyzer.analyse()(new WorkerID(workerId))
        currentWindow +=1
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
          tempProxy = new LiveProxy(jobID,superStep,timestamp,window)
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
