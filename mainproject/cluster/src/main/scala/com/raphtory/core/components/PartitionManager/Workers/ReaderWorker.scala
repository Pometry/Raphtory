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

class ReaderWorker(managerCountVal:Int,managerID:Int,workerId:Int,storage:EntityStorage)  extends Actor{
  implicit var managerCount: ManagerCount = ManagerCount(managerCountVal)
  val mediator: ActorRef = DistributedPubSub(context.system).mediator // get the mediator for sending cluster messages
  mediator ! DistributedPubSubMediator.Put(self)
  mediator ! DistributedPubSubMediator.Subscribe(Utils.readersWorkerTopic, self)
  var receivedMessages = AtomicInt(0)
  var tempProxy:LiveProxy = null

  override def receive: Receive = {
    case UpdatedCounter(newValue) => managerCount = ManagerCount(newValue)
    case Setup(analyzer,jobID,superStep,timestamp,analysisType,window,windowSet) => try{setup(analyzer,jobID,superStep,timestamp,analysisType,window,windowSet)}catch {case e:Exception => e.printStackTrace()}
    case CheckMessages(superstep) => checkMessages()
    case NextStep(analyzer,jobID,superStep,timestamp,analysisType,window,windowSet) => try{nextStep(analyzer,jobID,superStep,timestamp,analysisType,window,windowSet)}catch {case e:Exception => e.printStackTrace()}
    case NextStepNewAnalyser(name,jobID,currentStep,timestamp,analysisType,window,windowSet) => nextStepNewAnalyser(name,jobID,currentStep,timestamp,analysisType,window,windowSet)
    case Finish(analyzer,jobID,superStep,timestamp,analysisType,window,windowSet) =>   try{returnResults(analyzer,jobID,superStep,timestamp,analysisType,window,windowSet)}catch {case e:Exception => e.printStackTrace()}
    //case handler:VertexMessage => receivedMessage(handler)
    case VertexMessageFloat(source:Long,vertexID:Long,jobID:String,superStep:Int,data:Float) => receivedMessage(vertexID,jobID,superStep,data)
    case VertexMessageString(source:Long,vertexID:Long,jobID:String,superStep:Int,data:String) => receivedMessage(vertexID,jobID,superStep,data)
    case VertexMessageInt(source:Long,vertexID:Long,jobID:String,superStep:Int,data:Int) => receivedMessage(vertexID,jobID,superStep,data)
    case VertexMessageLong(source:Long,vertexID:Long,jobID:String,superStep:Int,data:Long) => receivedMessage(vertexID,jobID,superStep,data)
    case VertexMessageBatch(jobID:String,superStep:Int,data:Set[(Long,Long,Any)]) => data.foreach(f => receivedMessage(f._2,jobID,superStep,f._3))
  }

  def receivedMessage(vertexID:Long,jobID:String,superStep:Int,data:Any) = {
    receivedMessages.increment()
    storage.vertices(vertexID).multiQueue.receiveMessage(jobID,superStep,data)
  }

  def checkMessages() ={
    var count = AtomicInt(0)
    //tempProxy.getVerticesSet().foreach(v => count.add(tempProxy.getVertex(v._2).messageQueue2.size))
    sender() ! MessagesReceived(workerId,count.get,receivedMessages.get,tempProxy.getMessages())
  }

  def setup(analyzer: Analyser,jobID:String,superStep:Int,timestamp:Long,analysisType:AnalysisType.Value,window:Long,windowSet:Array[Long]) {
    receivedMessages.set(0)
    setProxy(jobID,superStep,timestamp,analysisType,window,windowSet)
    analyzer.sysSetup(context,managerCount,tempProxy,workerId)
    if(windowSet.isEmpty) {
      analyzer.setup()
      sender() ! Ready(tempProxy.getMessages())
    }
    else{
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
    setProxy(jobID,superStep,timestamp,analysisType,window,windowSet)
    analyzer.sysSetup(context,managerCount,tempProxy,workerId)
    if(windowSet.isEmpty) {
      analyzer.analyse()
      sender() ! EndStep(tempProxy.getMessages(),tempProxy.checkVotes(workerId))
    }
    else{
      val individualResults:mutable.ArrayBuffer[Any] = ArrayBuffer[Any]()
      analyzer.analyse()
      for(i<- windowSet.indices)
        if(i!=0) {
          tempProxy.asInstanceOf[WindowProxy].shrinkWindow(windowSet(i))
          analyzer.analyse()
        }
      sender() ! EndStep(tempProxy.getMessages(),tempProxy.checkVotes(workerId))
      }

  }

  def nextStepNewAnalyser(name: String,jobID:String,currentStep:Int,timestamp:Long,analysisType:AnalysisType.Value,window:Long,windowSet:Array[Long]) = {
    nextStep(Utils.analyserMap(name),jobID,currentStep,timestamp,analysisType,window,windowSet)
  }

  def returnResults(analyzer: Analyser,jobID:String,superStep:Int,timestamp:Long,analysisType:AnalysisType.Value,window:Long,windowSet:Array[Long]): Unit = {
    setProxy(jobID,superStep,timestamp,analysisType,window,windowSet)
    analyzer.sysSetup(context,managerCount,tempProxy,workerId)
    if(windowSet.isEmpty) {
      val result = analyzer.returnResults()
      sender() ! ReturnResults(result)
    }
    else{
      val individualResults:mutable.ArrayBuffer[Any] = ArrayBuffer[Any]()
      individualResults += analyzer.returnResults()
      for(i<- windowSet.indices)
        if(i!=0) {
          tempProxy.asInstanceOf[WindowProxy].shrinkWindow(windowSet(i))
          individualResults += analyzer.returnResults()
        }
      sender() ! ReturnResults(individualResults)
    }
  }





  private def setProxy(jobID:String,superStep:Int,timestamp:Long,analysisType:AnalysisType.Value,window:Long,windowSet:Array[Long]):Unit = {
    analysisType match {
      case AnalysisType.live => {
        if(windowSet.nonEmpty) //we have a set of windows to run
          tempProxy = new WindowProxy(jobID,superStep,storage.newestTime,windowSet(0),workerId,storage,managerCount)
        else if(window != -1) // we only have one window to run
          tempProxy = new WindowProxy(jobID,superStep,storage.newestTime,window,workerId,storage,managerCount)
        else
          tempProxy = new LiveProxy(jobID,superStep,timestamp,window,workerId,storage,managerCount)
      }
      case AnalysisType.view  => {
        if(windowSet.nonEmpty) //we have a set of windows to run
          tempProxy = new WindowProxy(jobID,superStep,timestamp,windowSet(0),workerId,storage,managerCount)
        else if(window != -1) // we only have one window to run
          tempProxy = new WindowProxy(jobID,superStep,timestamp,window,workerId,storage,managerCount)
        else
          tempProxy = new ViewProxy(jobID,superStep,timestamp,workerId,storage,managerCount)
      }
      case AnalysisType.range  => {
        if(windowSet.nonEmpty) //we have a set of windows to run
          tempProxy = new WindowProxy(jobID,superStep,timestamp,windowSet(0),workerId,storage,managerCount)
        else if(window != -1) // we only have one window to run
          tempProxy = new WindowProxy(jobID,superStep,timestamp,window,workerId,storage,managerCount)
        else
          tempProxy = new ViewProxy(jobID,superStep,timestamp,workerId,storage,managerCount)
      }
    }
  }

}
