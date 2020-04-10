package com.raphtory.core.components.Router

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.model.communication.{AllocateJob, EdgeAdd, EdgeAddWithProperties, EdgeDelete, GraphUpdate, TrackedEdgeAdd, TrackedEdgeAddWithProperties, TrackedEdgeDelete, TrackedVertexAdd, TrackedVertexAddWithProperties, TrackedVertexDelete, UpdatedCounter, VertexAdd, VertexAddWithProperties, VertexDelete}
import com.raphtory.core.model.graphentities.Vertex
import com.raphtory.core.utils.Utils.getManager

import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap
import scala.util.hashing.MurmurHash3

// TODO Add val name which sub classes that extend this trait must overwrite
//  e.g. BlockChainRouter val name = "Blockchain Router"
//  Log.debug that read 'Router' should then read 'Blockchain Router'
trait RouterWorker extends Actor with ActorLogging {

  val routerId: Int
  val workerID: Int
  private val messageIDs = mutable.HashMap[String, AtomicInteger]()
  /** Private and protected values */
  private var managerCount: Int = initialManagerCount

  protected def initialManagerCount: Int
  protected def parseTuple(value: Any)

  final protected val mediator = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)

  override def preStart(): Unit =
    log.debug("RouterWorker [{}] is being started.", routerId)

  override def receive: Receive = {
    case req: UpdatedCounter => processUpdatedCounterRequest(req)
    case req: AllocateJob    => processAllocateJobRequest(req)
    case x                   => log.warning("RouterWorker received unknown [{}] message.", x)
  }

  final protected def getManagerCount: Int = managerCount

  def assignID(uniqueChars: String): Long = MurmurHash3.stringHash(uniqueChars)

  def processUpdatedCounterRequest(req: UpdatedCounter): Unit = {
    log.debug(s"RouterWorker [{}] received [{}] request.", routerId, req)

    if (managerCount < req.newValue) managerCount = req.newValue
  }

  def processAllocateJobRequest(req: AllocateJob): Unit = {
    log.debug(s"RouterWorker [{}] received [{}] request.", routerId, req)

    parseTuple(req.record)
  }

  def sendGraphUpdate[T <: GraphUpdate](message: T): Unit = {
    val path = getManager(message.srcID, getManagerCount)
    val id = messageIDs.get(path) match {
      case Some(messageid) =>
        messageid.getAndIncrement()
      case None =>
        messageIDs put (path,new AtomicInteger(1))
        0
    }

    message match {
      case m:VertexAdd =>
        mediator ! DistributedPubSubMediator.Send(path ,TrackedVertexAdd(s"${routerId}_${workerID}",id,m) , localAffinity = false)
      case m:VertexAddWithProperties =>
        mediator ! DistributedPubSubMediator.Send(path ,TrackedVertexAddWithProperties(s"${routerId}_${workerID}",id,m) , localAffinity = false)
      case m:EdgeAdd =>
        mediator ! DistributedPubSubMediator.Send(path ,TrackedEdgeAdd(s"${routerId}_${workerID}",id,m) , localAffinity = false)
      case m:EdgeAddWithProperties =>
        mediator ! DistributedPubSubMediator.Send(path ,TrackedEdgeAddWithProperties(s"${routerId}_${workerID}",id,m) , localAffinity = false)
      case m:VertexDelete =>
        mediator ! DistributedPubSubMediator.Send(path ,TrackedVertexDelete(s"${routerId}_${workerID}",id,m) , localAffinity = false)
      case m:EdgeDelete =>
        mediator ! DistributedPubSubMediator.Send(path ,TrackedEdgeDelete(s"${routerId}_${workerID}",id,m) , localAffinity = false)
    }



    log.debug("RouterWorker sending message [{}] to PubSub", message)
  }

}
