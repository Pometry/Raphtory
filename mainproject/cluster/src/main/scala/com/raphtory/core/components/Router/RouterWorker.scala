package com.raphtory.core.components.Router

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{Actor, ActorLogging, Cancellable}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.model.communication.{AllocateTuple, EdgeAdd, EdgeAddWithProperties, EdgeDelete, GraphUpdate, RouterWorkerTimeSync, TrackedEdgeAdd, TrackedEdgeAddWithProperties, TrackedEdgeDelete, TrackedVertexAdd, TrackedVertexAddWithProperties, TrackedVertexDelete, UpdatedCounter, VertexAdd, VertexAddWithProperties, VertexDelete}
import com.raphtory.core.model.graphentities.Vertex
import com.raphtory.core.utils.{SchedulerUtil, Utils}
import com.raphtory.core.utils.Utils.getManager
import kamon.Kamon

import scala.concurrent.duration._
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.parallel.mutable.ParTrieMap
import scala.util.hashing.MurmurHash3

// TODO Add val name which sub classes that extend this trait must overwrite
//  e.g. BlockChainRouter val name = "Blockchain Router"
//  Log.debug that read 'Router' should then read 'Blockchain Router'
trait RouterWorker extends Actor with ActorLogging {

  val routerId: Int
  val workerID: Int
  var newestTime:Long = 0
  private val messageIDs = mutable.HashMap[String, AtomicInteger]()
  /** Private and protected values */
  private var managerCount: Int = initialManagerCount
  val writerArray = Utils.getAllWriterWorkers(managerCount)

  val routerWorkerUpdates = Kamon.counter("Raphtory_Router_Output").withTag("Router",routerId).withTag("Worker",workerID)


  protected def initialManagerCount: Int
  protected def parseTuple(value: Any)

  final protected val mediator = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)
  private val scheduledTaskMap: mutable.HashMap[String, Cancellable] = mutable.HashMap[String, Cancellable]()

  override def preStart(): Unit = {
    log.debug("RouterWorker [{}] is being started.", routerId)
    scheduleTasks()
  }



  private def scheduleTasks(): Unit = {
    log.debug("Preparing to schedule tasks in Spout.")

    val broadcastancellable =
      SchedulerUtil.scheduleTask(initialDelay = 5 seconds, interval = 5 second, receiver = self, message = "timeBroadcast")
    scheduledTaskMap.put("timeBroadcast", broadcastancellable)
  }
  override def postStop() {
    val allTasksCancelled = scheduledTaskMap.forall {
      case (key, task) =>
        SchedulerUtil.cancelTask(key, task)
    }
    if (!allTasksCancelled) log.warning("Failed to cancel all scheduled tasks post stop.")
  }

  override def receive: Receive = {
    case req: UpdatedCounter => processUpdatedCounterRequest(req)
    case req: AllocateTuple    => processAllocateJobRequest(req)
    case "timeBroadcast"     => broadcastToPartitions()
    case x                   => log.warning("RouterWorker received unknown [{}] message.", x)
  }

  private def broadcastToPartitions() = {
    for (worker <- writerArray ) {

      mediator ! DistributedPubSubMediator.Send(worker,RouterWorkerTimeSync(newestTime,s"${routerId}_${workerID}",getMessageIDForWriter(worker)),false)
    }
  }

  final protected def getManagerCount: Int = managerCount

  def assignID(uniqueChars: String): Long = MurmurHash3.stringHash(uniqueChars)

  def processUpdatedCounterRequest(req: UpdatedCounter): Unit = {
    log.debug(s"RouterWorker [{}] received [{}] request.", routerId, req)

    if (managerCount < req.newValue) managerCount = req.newValue
  }

  def processAllocateJobRequest(req: AllocateTuple): Unit = {
    log.debug(s"RouterWorker [{}] received [{}] request.", routerId, req)
    parseTuple(req.record)
    routerWorkerUpdates.increment()
  }

  def sendGraphUpdate[T <: GraphUpdate](message: T): Unit = {
    val path = getManager(message.srcID, getManagerCount)
    val id = getMessageIDForWriter(path)

    message match {
      case m:VertexAdd =>
        newestTime = m.msgTime
        mediator ! DistributedPubSubMediator.Send(path ,TrackedVertexAdd(s"${routerId}_${workerID}",id,m) , localAffinity = false)
      case m:VertexAddWithProperties =>
        newestTime = m.msgTime
        mediator ! DistributedPubSubMediator.Send(path ,TrackedVertexAddWithProperties(s"${routerId}_${workerID}",id,m) , localAffinity = false)
      case m:EdgeAdd =>
        newestTime = m.msgTime
        mediator ! DistributedPubSubMediator.Send(path ,TrackedEdgeAdd(s"${routerId}_${workerID}",id,m) , localAffinity = false)
      case m:EdgeAddWithProperties =>
        newestTime = m.msgTime
        mediator ! DistributedPubSubMediator.Send(path ,TrackedEdgeAddWithProperties(s"${routerId}_${workerID}",id,m) , localAffinity = false)
      case m:VertexDelete =>
        newestTime = m.msgTime
        mediator ! DistributedPubSubMediator.Send(path ,TrackedVertexDelete(s"${routerId}_${workerID}",id,m) , localAffinity = false)
      case m:EdgeDelete =>
        newestTime = m.msgTime
        mediator ! DistributedPubSubMediator.Send(path ,TrackedEdgeDelete(s"${routerId}_${workerID}",id,m) , localAffinity = false)
    }



    log.debug("RouterWorker sending message [{}] to PubSub", message)
  }
  private def getMessageIDForWriter(path:String) ={
    messageIDs.get(path) match {
      case Some(messageid) =>
        messageid.getAndIncrement()
      case None =>
        messageIDs put (path,new AtomicInteger(1))
        0
    }
  }

}
