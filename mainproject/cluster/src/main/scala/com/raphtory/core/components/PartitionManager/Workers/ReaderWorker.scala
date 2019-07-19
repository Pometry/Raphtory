package com.raphtory.core.components.PartitionManager.Workers

import akka.actor.{Actor, ActorPath, ActorRef}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.analysis.{Analyser, GraphRepoProxy, ManagerCount, WorkerID}
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
  val debug = false
  implicit val workerID: Int = workerId
  var receivedMessages = AtomicInt(0)
  var tempProxy:GraphRepoProxy = null

  override def receive: Receive = {
    case UpdatedCounter(newValue) => managerCount = newValue
    case Setup(analyzer,jobID,superStep) => setup(analyzer,jobID,superStep)
    case CheckMessages(superstep) => {
      var count = 0
      //tempProxy.getVerticesSet()(WorkerID(workerID)).foreach(v => count+=EntityStorage.vertices.get(v).get.vertexMultiQueue.evenMessageQueueMap.getOrElseUpdate("testName",mutable.ArrayStack[VertexMessage]()).size)
      tempProxy.getVerticesSet()(WorkerID(workerID)).foreach(v => count+=EntityStorage.vertices.get(v).get.vertexMultiQueue.oddMessageQueueMap.getOrElseUpdate("testName",mutable.ArrayStack[VertexMessage]()).size)
      sender() ! MessagesReceived(workerID,count,receivedMessages.get,tempProxy.getMessages())
    }
    case NextStep(analyzer,jobID,superStep) => nextStep(analyzer,jobID,superStep)
    case NextStepNewAnalyser(name,jobID,currentStep) => nextStepNewAnalyser(name,jobID,currentStep)
    case handler:MessageHandler => {
      receivedMessages.add(1)
      EntityStorage.vertices(handler.vertexID).vertexMultiQueue.receiveMessage(handler)
    }
  }

  def setup(analyzer: Analyser,jobID:String,superStep:Int) {
    val rebuildAnalyser = Utils.deserialise(Utils.serialise(analyzer)).asInstanceOf[Analyser]
    receivedMessages.set(0)
    tempProxy = new GraphRepoProxy(jobID,superStep)
    rebuildAnalyser.sysSetup(context,ManagerCount(managerCount),tempProxy)
    rebuildAnalyser.setup()(new WorkerID(workerID))
    sender() ! Ready(tempProxy.getMessages())
  }

  def nextStep(analyzer: Analyser,jobID:String,superStep:Int): Unit = {
    if(debug)println(s"Received new step for pm_$managerID")
    //try {
      val rebuildAnalyser = Utils.deserialise(Utils.serialise(analyzer)).asInstanceOf[Analyser]
      receivedMessages.set(0)
      tempProxy = new GraphRepoProxy(jobID,superStep)
      rebuildAnalyser.sysSetup(context,ManagerCount(managerCount),tempProxy)
      val senderPath = sender().path
      analyze(rebuildAnalyser,senderPath)

    //}
   // catch {
   //   case e: Exception => {
   //     println(e)
   //     sender() ! ExceptionInAnalysis(e.toString)
   //   }
  //  }
  }

  private def analyze(analyzer: Analyser, senderPath: ActorPath) = {
    val value = analyzer.analyse()(new WorkerID(workerID))
    if(debug)println("StepEnd success. Sending to " + senderPath.toStringWithoutAddress)
    mediator ! DistributedPubSubMediator.Send(senderPath.toStringWithoutAddress, EndStep(value,tempProxy.getMessages()), false)

  }


  def nextStepNewAnalyser(name: String,jobID:String,currentStep:Int) = {
    nextStep(Utils.analyserMap(name),jobID,currentStep)
  }

}
