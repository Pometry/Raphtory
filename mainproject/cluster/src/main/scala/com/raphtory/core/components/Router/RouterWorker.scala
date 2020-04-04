package com.raphtory.core.components.Router

import akka.actor.Actor
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.model.communication.AllocateJob
import com.raphtory.core.model.communication.GraphUpdate
import com.raphtory.core.model.communication.UpdatedCounter
import com.raphtory.core.utils.Utils.getManager

import scala.util.hashing.MurmurHash3

trait RouterWorker extends Actor {
  final protected val mediator = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)
  val debug = System.getenv().getOrDefault("DEBUG", "false").trim.toBoolean
  protected def initialManagerCount: Int
  protected def parseTuple(value: Any)

  private var managerCount: Int       = initialManagerCount
  final protected def getManagerCount = managerCount
  private var count                   = 0

  override def receive = {
    case UpdatedCounter(newValue) => newPmJoined(newValue)
    case AllocateJob(record)      => parseTuple(record)
  }
  def assignID(uniqueChars: String): Long = MurmurHash3.stringHash(uniqueChars)
  def sendGraphUpdate[T <: GraphUpdate](message: T): Unit = {
    mediator ! DistributedPubSubMediator.Send(getManager(message.srcID, getManagerCount), message, false);
    //if (debug) println("router send update to pm")
  }
  private def newPmJoined(newValue: Int) = if (managerCount < newValue) managerCount = newValue

}
